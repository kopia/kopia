package repo

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/kopia/kopia/internal/jsonstream"
)

// ObjectWriter allows writing content to the storage and supports automatic deduplication and encryption
// of written data.
type ObjectWriter interface {
	io.WriteCloser

	Result() (ObjectID, error)
	StorageBlocks() []string
}

type blockTracker struct {
	mu     sync.Mutex
	blocks map[string]bool
}

func (t *blockTracker) addBlock(blockID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.blocks == nil {
		t.blocks = make(map[string]bool)
	}
	t.blocks[blockID] = true
}

func (t *blockTracker) blockIDs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()

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

	prefix          string
	currentPosition int64
	blockIndex      []indirectObjectEntry

	description string

	blockTracker *blockTracker
	splitter     objectSplitter

	disablePacking bool
	packGroup      string

	pendingBlocksWG sync.WaitGroup

	err asyncErrors
}

func (w *objectWriter) Close() error {
	w.pendingBlocksWG.Wait()
	return w.err.check()
}

func (w *objectWriter) Write(data []byte) (n int, err error) {
	dataLen := len(data)
	w.totalLength += int64(dataLen)

	for _, d := range data {
		w.buffer.WriteByte(d)

		if w.splitter.add(d) {
			if err := w.flushBuffer(); err != nil {
				return 0, err
			}
		}
	}

	return dataLen, nil
}

func (w *objectWriter) flushBuffer() error {
	length := w.buffer.Len()
	chunkID := len(w.blockIndex)
	w.blockIndex = append(w.blockIndex, indirectObjectEntry{})
	w.blockIndex[chunkID].Start = w.currentPosition
	w.blockIndex[chunkID].Length = int64(length)
	w.currentPosition += int64(length)

	var b2 bytes.Buffer
	w.buffer.WriteTo(&b2)
	w.buffer.Reset()

	do := func() {
		objectID, err := w.repo.hashEncryptAndWrite(w.packGroup, &b2, w.prefix, w.disablePacking)
		w.repo.trace("OBJECT_WRITER(%q) stored %v (%v bytes)", w.description, objectID, length)
		if err != nil {
			w.err.add(fmt.Errorf("error when flushing chunk %d of %s: %v", chunkID, w.description, err))
			return
		}

		w.blockTracker.addBlock(objectID.StorageBlock)
		w.blockIndex[chunkID].Object = objectID
	}

	if w.repo.async {
		w.repo.writeBackSemaphore.Lock()
		w.pendingBlocksWG.Add(1)
		w.repo.writeBackWG.Add(1)

		go func() {
			defer w.pendingBlocksWG.Done()
			defer w.repo.writeBackWG.Done()
			defer w.repo.writeBackSemaphore.Unlock()
			do()
		}()

		return nil
	}

	do()
	return w.err.check()
}

func (w *objectWriter) Result() (ObjectID, error) {
	if w.buffer.Len() > 0 || len(w.blockIndex) == 0 {
		w.flushBuffer()
	}
	w.pendingBlocksWG.Wait()

	if err := w.err.check(); err != nil {
		return NullObjectID, err
	}

	if len(w.blockIndex) == 1 {
		return w.blockIndex[0].Object, nil
	}

	iw := &objectWriter{
		repo:         w.repo,
		prefix:       w.prefix,
		description:  "LIST(" + w.description + ")",
		blockTracker: w.blockTracker,
		splitter:     w.repo.newSplitter(),

		disablePacking: w.disablePacking,
		packGroup:      w.packGroup,
	}

	jw := jsonstream.NewWriter(iw, indirectStreamType)
	for _, e := range w.blockIndex {
		jw.Write(&e)
	}
	jw.Finalize()
	oid, err := iw.Result()
	if err != nil {
		return NullObjectID, err
	}
	return ObjectID{Indirect: &oid}, nil
}

func (w *objectWriter) StorageBlocks() []string {
	w.pendingBlocksWG.Wait()
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
