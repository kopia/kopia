package object

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/splitter"
)

var log = logging.Module("object")

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
	mu sync.Mutex
	// +checklocks:mu
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
	// objectWriter implements io.Writer but needs context to talk to repository
	ctx context.Context //nolint:containedctx

	om *Manager

	compressor         compression.Compressor
	metadataCompressor compression.Compressor

	prefix      content.IDPrefix
	buffer      gather.WriteBuffer
	totalLength int64

	currentPosition int64

	indirectIndexGrowMutex sync.Mutex
	indirectIndex          []IndirectObjectEntry
	indirectIndexBuf       [4]IndirectObjectEntry // small buffer so that we avoid allocations most of the time

	description string

	splitter splitter.Splitter

	// provides mutual exclusion of all public APIs (Write, Result, Checkpoint)
	mu sync.Mutex

	asyncWritesSemaphore chan struct{} // async writes semaphore or  nil
	asyncWritesWG        sync.WaitGroup

	contentWriteErrorMutex sync.Mutex
	contentWriteError      error // stores async write error, propagated in Result()
}

func (w *objectWriter) Close() error {
	// wait for any async writes to complete
	w.asyncWritesWG.Wait()

	if w.splitter != nil {
		w.splitter.Close()
	}

	w.buffer.Close()

	w.om.closedWriter(w)

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
			w.buffer.Append(data)
			break
		}

		// found a split point after `n` bytes, write first n bytes then flush and repeat with the remainder.
		w.buffer.Append(data[0:n])

		if err := w.flushBuffer(); err != nil {
			return 0, err
		}

		data = data[n:]
	}

	return dataLen, nil
}

func (w *objectWriter) flushBuffer() error {
	length := w.buffer.Length()

	// hold a lock as we may grow the index
	w.indirectIndexGrowMutex.Lock()
	chunkID := len(w.indirectIndex)
	w.indirectIndex = append(w.indirectIndex, IndirectObjectEntry{})
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

	asyncBuf := gather.NewWriteBuffer()
	w.buffer.Bytes().WriteTo(asyncBuf) //nolint:errcheck

	go func() {
		defer func() {
			// release write semaphore and buffer
			<-w.asyncWritesSemaphore
			asyncBuf.Close()
			w.asyncWritesWG.Done()
		}()

		if err := w.prepareAndWriteContentChunk(chunkID, asyncBuf.Bytes()); err != nil {
			log(w.ctx).Errorf("async write error: %v", err)

			_ = w.saveError(err)
		}
	}()

	return nil
}

func (w *objectWriter) prepareAndWriteContentChunk(chunkID int, data gather.Bytes) error {
	var b gather.WriteBuffer
	defer b.Close()

	// allocate buffer to hold either compressed bytes or the uncompressed
	comp := content.NoCompression
	objectComp := w.compressor

	// in super rare cases this may be stale, but if it is it will be false which is always safe.
	supportsContentCompression := w.om.contentMgr.SupportsContentCompression()

	// do not compress in this layer, instead pass comp to the content manager.
	if supportsContentCompression && w.compressor != nil {
		comp = w.compressor.HeaderID()
		objectComp = nil
	}

	// metadata objects are ALWAYS compressed at the content layer, irrespective of the index version (1 or 1+).
	// even if a compressor for metadata objects is set by the caller, do not compress the objects at this layer;
	// instead, let it be handled at the content layer.
	if w.prefix != "" {
		objectComp = nil
	}

	// contentBytes is what we're going to write to the content manager, it potentially uses bytes from b
	contentBytes, isCompressed, err := maybeCompressedContentBytes(objectComp, data, &b)
	if err != nil {
		return errors.Wrap(err, "unable to prepare content bytes")
	}

	contentID, err := w.om.contentMgr.WriteContent(w.ctx, contentBytes, w.prefix, comp)
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

func maybeCompressedContentBytes(comp compression.Compressor, input gather.Bytes, output *gather.WriteBuffer) (data gather.Bytes, isCompressed bool, err error) {
	if comp != nil {
		if err := comp.Compress(output, input.Reader()); err != nil {
			return gather.Bytes{}, false, errors.Wrap(err, "compression error")
		}

		if output.Length() < input.Length() {
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
	if w.buffer.Length() > 0 || len(w.indirectIndex) == 0 {
		if err := w.flushBuffer(); err != nil {
			return EmptyID, err
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
		return EmptyID, w.contentWriteError
	}

	if len(w.indirectIndex) == 0 {
		return EmptyID, nil
	}

	if len(w.indirectIndex) == 1 {
		return w.indirectIndex[0].Object, nil
	}

	iw := &objectWriter{
		ctx:                w.ctx,
		om:                 w.om,
		compressor:         w.metadataCompressor,
		metadataCompressor: w.metadataCompressor,
		description:        "LIST(" + w.description + ")",
		splitter:           w.om.newDefaultSplitter(),
		prefix:             w.prefix,
	}

	if iw.prefix == "" {
		// force a prefix for indirect contents to make sure they get packaged into metadata (q) blobs.
		iw.prefix = indirectContentPrefix
	}

	defer iw.Close() //nolint:errcheck

	if err := writeIndirectObject(iw, w.indirectIndex); err != nil {
		return EmptyID, err
	}

	oid, err := iw.Result()
	if err != nil {
		return EmptyID, err
	}

	return IndirectObjectID(oid), nil
}

func writeIndirectObject(w io.Writer, entries []IndirectObjectEntry) error {
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
	Description        string
	Prefix             content.IDPrefix // empty string or a single-character ('g'..'z')
	Compressor         compression.Name
	MetadataCompressor compression.Name
	Splitter           string // use particular splitter instead of default
	AsyncWrites        int    // allow up to N content writes to be asynchronous
}
