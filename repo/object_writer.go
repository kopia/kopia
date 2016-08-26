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
	repo *Repository

	buffer      *bytes.Buffer
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
}

func (w *objectWriter) Close() error {
	if w.buffer != nil {
		w.repo.bufferManager.returnBuffer(w.buffer)
		w.buffer = nil
	}

	if w.listWriter != nil {
		w.listWriter.Close()
		w.listWriter = nil
	}
	return nil
}

func (w *objectWriter) WriteGather(dataSlices [][]byte) (n int, err error) {
	dataLen := 0
	for _, s := range dataSlices {
		dataLen += len(s)
	}

	remaining := dataLen
	w.totalLength += int64(remaining)

	for remaining > 0 {
		if w.buffer == nil {
			w.buffer = w.repo.bufferManager.newBuffer()
		}
		room := w.buffer.Cap() - w.buffer.Len()

		if remaining <= room {
			for _, s := range dataSlices {
				w.buffer.Write(s)
			}
			dataSlices = nil
			remaining = 0
		} else {
			for room > 0 {
				var chunk []byte

				if len(dataSlices[0]) >= room {
					chunk = dataSlices[0][0:room]
					dataSlices[0] = dataSlices[0][len(chunk):]
				} else {
					chunk = dataSlices[0]
					dataSlices[0] = nil
				}
				w.buffer.Write(chunk)
				remaining -= len(chunk)
				room -= len(chunk)
				if len(dataSlices[0]) == 0 {
					dataSlices = dataSlices[1:]
				}
			}

			if err := w.flushBuffer(false); err != nil {
				return 0, err
			}
		}
	}
	return dataLen, nil
}

func (w *objectWriter) Write(data []byte) (n int, err error) {
	var b [1][]byte

	b[0] = data
	return w.WriteGather(b[:])
}

func (w *objectWriter) flushBuffer(force bool) error {
	// log.Printf("flushing bufer")
	// defer log.Printf("flushed")
	if w.buffer != nil || force {
		var length int
		if w.buffer != nil {
			length = w.buffer.Len()
		}

		b := w.buffer
		w.buffer = nil

		objectID, err := w.repo.hashEncryptAndWriteMaybeAsync(b, w.prefix)
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
	}
	return nil
}

func (w *objectWriter) Result(forceStored bool) (ObjectID, error) {
	if !forceStored && w.flushedObjectCount == 0 {
		if w.buffer == nil {
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
		w.lastFlushedObject.Indirect = w.indirectLevel
		return w.lastFlushedObject, nil
	} else if w.flushedObjectCount == 0 {
		return NullObjectID, nil
	} else {
		w.listProtoWriter.Close()
		return w.listWriter.Result(true)
	}
}

func (w *objectWriter) StorageBlocks() []string {
	return w.blockTracker.blockIDs()
}

// WriterOption is an option that can be passed to Repository.NewWriter()
type WriterOption func(*objectWriter)

// WithBlockNamePrefix causes the ObjectWriter to prefix any blocks emitted to the storage with a given string.
func WithBlockNamePrefix(prefix string) WriterOption {
	return func(w *objectWriter) {
		w.prefix = prefix
	}
}

// WithDescription is used for debugging only and causes the following string to be emitted in logs.
func WithDescription(description string) WriterOption {
	return func(w *objectWriter) {
		w.description = description
	}
}
