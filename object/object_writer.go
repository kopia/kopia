package object

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/kopia/repo/internal/jsonstream"
)

// Writer allows writing content to the storage and supports automatic deduplication and encryption
// of written data.
type Writer interface {
	io.WriteCloser

	Result() (ID, error)
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
	ctx  context.Context
	repo *Manager

	prefix      string
	buffer      bytes.Buffer
	totalLength int64

	currentPosition int64
	blockIndex      []indirectObjectEntry

	description string

	splitter        objectSplitter
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
	w.buffer.WriteTo(&b2) //nolint:errcheck
	w.buffer.Reset()

	do := func() {
		blockID, err := w.repo.blockMgr.WriteBlock(w.ctx, b2.Bytes(), w.prefix)
		w.repo.trace("OBJECT_WRITER(%q) stored %v (%v bytes)", w.description, blockID, length)
		if err != nil {
			w.err.add(fmt.Errorf("error when flushing chunk %d of %s: %v", chunkID, w.description, err))
			return
		}

		w.blockIndex[chunkID].Object = DirectObjectID(blockID)
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

func (w *objectWriter) Result() (ID, error) {
	if w.buffer.Len() > 0 || len(w.blockIndex) == 0 {
		if err := w.flushBuffer(); err != nil {
			return "", err
		}
	}
	w.pendingBlocksWG.Wait()

	if err := w.err.check(); err != nil {
		return "", err
	}

	if len(w.blockIndex) == 1 {
		return w.blockIndex[0].Object, nil
	}

	iw := &objectWriter{
		ctx:         w.ctx,
		repo:        w.repo,
		description: "LIST(" + w.description + ")",
		splitter:    w.repo.newSplitter(),
		prefix:      w.prefix,
	}

	jw := jsonstream.NewWriter(iw, indirectStreamType)
	for _, e := range w.blockIndex {
		if err := jw.Write(&e); err != nil {
			return "", fmt.Errorf("unable to write indirect block index: %v", err)
		}
	}
	if err := jw.Finalize(); err != nil {
		return "", fmt.Errorf("unable to finalize indirect block index: %v", err)
	}
	oid, err := iw.Result()
	if err != nil {
		return "", err
	}
	return IndirectObjectID(oid), nil
}

// WriterOptions can be passed to Repository.NewWriter()
type WriterOptions struct {
	Description string
	Prefix      string // empty string or a single-character ('g'..'z')

	splitter objectSplitter
}
