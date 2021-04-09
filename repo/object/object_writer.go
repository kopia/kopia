package object

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/buf"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/splitter"
)

var log = logging.GetContextLoggerFunc("object")

const indirectContentPrefix = "x"

// Writer allows writing content to the storage and supports automatic deduplication and encryption
// of written data.
type Writer interface {
	io.WriteCloser

	// Checkpoint returns ID of an object consisting of all contents written to storage so far.
	// This may not include some data buffered in the writer.
	// In case nothing has been written yet, returns empty object ID.
	Checkpoint() (ID, error)

	// Result returns object ID representing all bytes written to the writer.
	Result() (ID, error)
}

type contentIDTracker struct {
	mu       sync.Mutex
	contents map[content.ID]bool
}

func (t *contentIDTracker) addContentID(contentID content.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.contents == nil {
		t.contents = make(map[content.ID]bool)
	}

	t.contents[contentID] = true
}

func (t *contentIDTracker) contentIDs() []content.ID {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make([]content.ID, 0, len(t.contents))
	for k := range t.contents {
		result = append(result, k)
	}

	return result
}

type objectWriter struct {
	ctx context.Context
	om  *Manager

	compressor compression.Compressor

	prefix      content.ID
	buf         buf.Buf
	buffer      *bytes.Buffer
	totalLength int64

	currentPosition int64

	indirectIndexGrowMutex sync.Mutex
	indirectIndex          []indirectObjectEntry
	indirectIndexBuf       [4]indirectObjectEntry // small buffer so that we avoid allocations most of the time

	description string

	splitter splitter.Splitter

	// provides mutual exclusion of all public APIs (Write, Result, Checkpoint)
	mu sync.Mutex

	asyncWritesSemaphore chan struct{} // async writes semaphore or  nil
	asyncWritesWG        sync.WaitGroup

	contentWriteErrorMutex sync.Mutex
	contentWriteError      error // stores async write error, propagated in Result()
}

func (w *objectWriter) initBuffer() {
	w.buf = w.om.bufferPool.Allocate(w.splitter.MaxSegmentSize())
	w.buffer = bytes.NewBuffer(w.buf.Data[:0])
}

func (w *objectWriter) Close() error {
	// wait for any async writes to complete
	w.asyncWritesWG.Wait()

	w.buf.Release()

	if w.splitter != nil {
		w.splitter.Close()
	}

	return nil
}

func (w *objectWriter) Write(data []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	dataLen := len(data)
	w.totalLength += int64(dataLen)

	for len(data) > 0 {
		n := w.splitter.NextSplitPoint(data)
		if n < 0 {
			// no split points in the buffer
			w.buffer.Write(data)
			break
		}

		// found a split point after `n` bytes, write first n bytes then flush and repeat with the remainder.
		w.buffer.Write(data[0:n])

		if err := w.flushBuffer(); err != nil {
			return 0, err
		}

		data = data[n:]
	}

	return dataLen, nil
}

func (w *objectWriter) flushBuffer() error {
	length := w.buffer.Len()

	// hold a lock as we may grow the index
	w.indirectIndexGrowMutex.Lock()
	chunkID := len(w.indirectIndex)
	w.indirectIndex = append(w.indirectIndex, indirectObjectEntry{})
	w.indirectIndex[chunkID].Start = w.currentPosition
	w.indirectIndex[chunkID].Length = int64(length)
	w.currentPosition += int64(length)
	w.indirectIndexGrowMutex.Unlock()

	defer w.buffer.Reset()

	if w.asyncWritesSemaphore == nil {
		return w.saveError(w.prepareAndWriteContentChunk(chunkID, w.buffer.Bytes()))
	}

	// acquire write semaphore
	w.asyncWritesSemaphore <- struct{}{}
	w.asyncWritesWG.Add(1)

	asyncBuf := w.om.bufferPool.Allocate(length)
	asyncBytes := append(asyncBuf.Data[:0], w.buffer.Bytes()...)

	go func() {
		defer func() {
			// release write semaphore and buffer
			<-w.asyncWritesSemaphore
			asyncBuf.Release()
			w.asyncWritesWG.Done()
		}()

		if err := w.prepareAndWriteContentChunk(chunkID, asyncBytes); err != nil {
			log(w.ctx).Errorf("async write error: %v", err)

			_ = w.saveError(err)
		}
	}()

	return nil
}

func (w *objectWriter) prepareAndWriteContentChunk(chunkID int, data []byte) error {
	// allocate buffer to hold either compressed bytes or the uncompressed
	b := w.om.bufferPool.Allocate(len(data) + maxCompressionOverheadPerSegment)
	defer b.Release()

	// contentBytes is what we're going to write to the content manager, it potentially uses bytes from b
	contentBytes, isCompressed, err := maybeCompressedContentBytes(w.compressor, bytes.NewBuffer(b.Data[:0]), data)
	if err != nil {
		return errors.Wrap(err, "unable to prepare content bytes")
	}

	contentID, err := w.om.contentMgr.WriteContent(w.ctx, contentBytes, w.prefix)
	if err != nil {
		return errors.Wrapf(err, "unable to write content chunk %v of %v: %v", chunkID, w.description, err)
	}

	// update index under a lock
	w.indirectIndexGrowMutex.Lock()
	w.indirectIndex[chunkID].Object = maybeCompressedObjectID(contentID, isCompressed)
	w.indirectIndexGrowMutex.Unlock()

	return nil
}

func (w *objectWriter) saveError(err error) error {
	if err != nil {
		// store write error so that we fail at Result() later.
		w.contentWriteErrorMutex.Lock()
		w.contentWriteError = err
		w.contentWriteErrorMutex.Unlock()
	}

	return err
}

func maybeCompressedObjectID(contentID content.ID, isCompressed bool) ID {
	oid := DirectObjectID(contentID)

	if isCompressed {
		oid = Compressed(oid)
	}

	return oid
}

func maybeCompressedContentBytes(comp compression.Compressor, output *bytes.Buffer, input []byte) (data []byte, isCompressed bool, err error) {
	if comp != nil {
		if err := comp.Compress(output, input); err != nil {
			return nil, false, errors.Wrap(err, "compression error")
		}

		if output.Len() < len(input) {
			return output.Bytes(), true, nil
		}
	}

	return input, false, nil
}

func (w *objectWriter) Result() (ID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// no need to hold a lock on w.indirectIndexGrowMutex, since growing index only happens synchronously
	// and never in parallel with calling Result()
	if w.buffer.Len() > 0 || len(w.indirectIndex) == 0 {
		if err := w.flushBuffer(); err != nil {
			return "", err
		}
	}

	return w.checkpointLocked()
}

// Checkpoint returns object ID which represents portion of the object that has already been written.
// The result may be an empty object ID if nothing has been flushed yet.
func (w *objectWriter) Checkpoint() (ID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.checkpointLocked()
}

func (w *objectWriter) checkpointLocked() (ID, error) {
	// wait for any in-flight asynchronous writes to finish
	w.asyncWritesWG.Wait()

	if w.contentWriteError != nil {
		return "", w.contentWriteError
	}

	if len(w.indirectIndex) == 0 {
		return "", nil
	}

	if len(w.indirectIndex) == 1 {
		return w.indirectIndex[0].Object, nil
	}

	iw := &objectWriter{
		ctx:         w.ctx,
		om:          w.om,
		compressor:  nil,
		description: "LIST(" + w.description + ")",
		splitter:    w.om.newSplitter(),
		prefix:      w.prefix,
	}

	if iw.prefix == "" {
		// force a prefix for indirect contents to make sure they get packaged into metadata (q) blobs.
		iw.prefix = indirectContentPrefix
	}

	iw.initBuffer()

	defer iw.Close() //nolint:errcheck

	if err := writeIndirectObject(iw, w.indirectIndex); err != nil {
		return "", err
	}

	oid, err := iw.Result()
	if err != nil {
		return "", err
	}

	return IndirectObjectID(oid), nil
}

func writeIndirectObject(w io.Writer, entries []indirectObjectEntry) error {
	ind := indirectObject{
		StreamID: "kopia:indirect",
		Entries:  entries,
	}

	if err := json.NewEncoder(w).Encode(ind); err != nil {
		return errors.Wrap(err, "unable to write indirect object index")
	}

	return nil
}

// WriterOptions can be passed to Repository.NewWriter().
type WriterOptions struct {
	Description string
	Prefix      content.ID // empty string or a single-character ('g'..'z')
	Compressor  compression.Name
	AsyncWrites int // allow up to N content writes to be asynchronous
}
