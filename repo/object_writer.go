package repo

import (
	"bytes"
	"fmt"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
)

// ObjectWriter allows writing content to the storage and supports automatic deduplication and encryption
// of written data.
type ObjectWriter interface {
	io.WriteCloser

	Result(forceStored bool) (ObjectID, error)
	StorageBlocks() []string
}

type blockTracker struct {
	blocks map[string]bool
}

func (t *blockTracker) addBlock(blockID string) {
	if t.blocks == nil {
		t.blocks = make(map[string]bool)
	}
	t.blocks[blockID] = true
}

func (t *blockTracker) blockIDs() []string {
	result := make([]string, 0, len(t.blocks))
	for k := range t.blocks {
		result = append(result, k)
	}
	return result
}

type objectWriter struct {
	repo *ObjectManager

	buffer      bytes.Buffer
	totalLength int64

	prefix             string
	listWriter         *objectWriter
	listProtoWriter    *jsonstream.Writer
	listCurrentPos     int64
	flushedObjectCount int
	lastFlushedObject  ObjectID

	description   string
	indirectLevel int32

	blockTracker *blockTracker
	splitter     objectSplitter

	disablePacking bool
	packGroup      string
}

func (w *objectWriter) Close() error {
	if w.listWriter != nil {
		w.listWriter.Close()
		w.listWriter = nil
	}
	return nil
}

func (w *objectWriter) Write(data []byte) (n int, err error) {
	dataLen := len(data)
	w.totalLength += int64(dataLen)

	for _, d := range data {
		w.buffer.WriteByte(d)

		if w.splitter.add(d) {
			if err := w.flushBuffer(false); err != nil {
				return 0, err
			}
		}
	}

	return dataLen, nil
}

func (w *objectWriter) flushBuffer(force bool) error {
	if !force && w.buffer.Len() == 0 {
		return nil
	}

	length := w.buffer.Len()

	var b2 bytes.Buffer
	w.buffer.WriteTo(&b2)
	w.buffer.Reset()

	objectID, err := w.repo.hashEncryptAndWriteMaybeAsync(w.packGroup, &b2, w.prefix, w.disablePacking)
	if err != nil {
		return fmt.Errorf(
			"error when flushing chunk %d of %s: %#v",
			w.flushedObjectCount,
			w.description,
			err)
	}

	w.blockTracker.addBlock(objectID.StorageBlock)

	w.flushedObjectCount++
	w.lastFlushedObject = objectID
	if w.listWriter == nil {
		w.listWriter = &objectWriter{
			repo:          w.repo,
			indirectLevel: w.indirectLevel + 1,
			prefix:        w.prefix,
			description:   "LIST(" + w.description + ")",
			blockTracker:  w.blockTracker,
			splitter:      w.repo.newSplitter(),

			disablePacking: w.disablePacking,
			packGroup:      w.packGroup,
		}
		w.listProtoWriter = jsonstream.NewWriter(w.listWriter, indirectStreamType)
		w.listCurrentPos = 0
	}

	w.listProtoWriter.Write(&indirectObjectEntry{
		Object: &objectID,
		Start:  w.listCurrentPos,
		Length: int64(length),
	})

	w.listCurrentPos += int64(length)
	return nil
}

func (w *objectWriter) Result(forceStored bool) (ObjectID, error) {
	if !forceStored && w.flushedObjectCount == 0 {
		if w.buffer.Len() == 0 {
			return NullObjectID, nil
		}

		if w.buffer.Len() < int(w.repo.format.MaxInlineContentLength) {
			return InlineObjectID(w.buffer.Bytes()), nil
		}
	}

	w.flushBuffer(forceStored)
	defer func() {
		if w.listWriter != nil {
			w.listWriter.Close()
		}
	}()

	if w.flushedObjectCount == 1 {
		w.lastFlushedObject = addIndirection(w.lastFlushedObject, w.indirectLevel)
		return w.lastFlushedObject, nil
	} else if w.flushedObjectCount == 0 {
		return NullObjectID, nil
	} else {
		w.listProtoWriter.Finalize()
		return w.listWriter.Result(true)
	}
}

func addIndirection(oid ObjectID, level int32) ObjectID {
	if level == 0 {
		return oid
	}

	return addIndirection(ObjectID{Indirect: &oid}, level-1)
}

func (w *objectWriter) StorageBlocks() []string {
	return w.blockTracker.blockIDs()
}

// WriterOptions can be passed to Repository.NewWriter()
type WriterOptions struct {
	BlockNamePrefix string
	Description     string
	PackGroup       string

	splitter       objectSplitter
	disablePacking bool
}
