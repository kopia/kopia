// Package object implements repository support for content-addressable objects of arbitrary size.
package object

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/splitter"
)

// ErrObjectNotFound is returned when an object cannot be found.
var ErrObjectNotFound = errors.New("object not found")

// Reader allows reading, seeking, getting the length of and closing of a repository object.
type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

type contentReader interface {
	ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error)
	GetContent(ctx context.Context, contentID content.ID) ([]byte, error)
	PrefetchContents(ctx context.Context, contentIDs []content.ID, prefetchHint string) []content.ID
}

type contentManager interface {
	contentReader

	SupportsContentCompression() bool
	WriteContent(ctx context.Context, data gather.Bytes, prefix content.IDPrefix, comp compression.HeaderID) (content.ID, error)
}

// Manager implements a content-addressable storage on top of blob storage.
type Manager struct {
	Format format.ObjectFormat

	contentMgr         contentManager
	newDefaultSplitter splitter.Factory
	writerPool         sync.Pool
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (om *Manager) NewWriter(ctx context.Context, opt WriterOptions) Writer {
	w, _ := om.writerPool.Get().(*objectWriter)
	w.ctx = ctx
	w.om = om

	var splitFactory splitter.Factory

	if opt.Splitter != "" {
		splitFactory = splitter.GetFactory(opt.Splitter)
	}

	if splitFactory == nil {
		splitFactory = om.newDefaultSplitter
	}

	w.splitter = splitFactory()

	w.description = opt.Description
	w.prefix = opt.Prefix
	w.compressor = compression.ByName[opt.Compressor]
	w.metadataCompressor = compression.ByName[opt.MetadataCompressor]
	w.totalLength = 0
	w.currentPosition = 0

	// point the slice at the embedded array, so that we avoid allocations most of the time
	w.indirectIndex = w.indirectIndexBuf[:0]

	if opt.AsyncWrites > 0 {
		if len(w.asyncWritesSemaphore) != 0 || cap(w.asyncWritesSemaphore) != opt.AsyncWrites {
			w.asyncWritesSemaphore = make(chan struct{}, opt.AsyncWrites)
		}
	} else {
		w.asyncWritesSemaphore = nil
	}

	w.buffer.Reset()
	w.contentWriteError = nil

	return w
}

func (om *Manager) closedWriter(ow *objectWriter) {
	om.writerPool.Put(ow)
}

// Concatenate creates an object that's a result of concatenation of other objects. This is more efficient than reading
// and rewriting the objects because Concatenate can efficiently merge index entries without reading the underlying
// contents.
//
// This function exists primarily to facilitate efficient parallel uploads of very large files (>1GB). Due to bottleneck of
// splitting which is inherently sequential, we can only one use CPU core for each Writer, which limits throughput.
//
// For example when uploading a 100 GB file it is beneficial to independently upload sections of [0..25GB),
// [25..50GB), [50GB..75GB) and [75GB..100GB) and concatenate them together as this allows us to run four splitters
// in parallel utilizing more CPU cores. Because some split points now start at fixed boundaries and not content-specific,
// this causes some slight loss of deduplication at concatenation points (typically 1-2 contents, usually <10MB),
// so this method should only be used for very large files where this overhead is relatively small.
func (om *Manager) Concatenate(ctx context.Context, objectIDs []ID, metadataComp compression.Name) (ID, error) {
	if len(objectIDs) == 0 {
		return EmptyID, errors.New("empty list of objects")
	}

	if len(objectIDs) == 1 {
		return objectIDs[0], nil
	}

	var (
		concatenatedEntries []IndirectObjectEntry
		totalLength         int64
		err                 error
	)

	for _, objectID := range objectIDs {
		concatenatedEntries, totalLength, err = appendIndexEntriesForObject(ctx, om.contentMgr, concatenatedEntries, totalLength, objectID)
		if err != nil {
			return EmptyID, errors.Wrapf(err, "error appending %v", objectID)
		}
	}

	log(ctx).Debugf("concatenated: %v total: %v", concatenatedEntries, totalLength)

	w := om.NewWriter(ctx, WriterOptions{
		Prefix:             indirectContentPrefix,
		Description:        "CONCATENATED INDEX",
		Compressor:         metadataComp,
		MetadataCompressor: metadataComp,
	})
	defer w.Close() //nolint:errcheck

	if werr := writeIndirectObject(w, concatenatedEntries); werr != nil {
		return EmptyID, werr
	}

	concatID, err := w.Result()
	if err != nil {
		return EmptyID, errors.Wrap(err, "error writing concatenated index")
	}

	return IndirectObjectID(concatID), nil
}

func appendIndexEntriesForObject(ctx context.Context, cr contentReader, indexEntries []IndirectObjectEntry, startingLength int64, objectID ID) (result []IndirectObjectEntry, totalLength int64, _ error) {
	if indexObjectID, ok := objectID.IndexObjectID(); ok {
		ndx, err := LoadIndexObject(ctx, cr, indexObjectID)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error reading index of %v", objectID)
		}

		indexEntries, totalLength = appendIndexEntries(indexEntries, startingLength, ndx...)

		return indexEntries, totalLength, nil
	}

	// non-index object - the precise length of the object cannot be determined from content due to compression and padding,
	// so we must open the object to read its length.
	r, err := Open(ctx, cr, objectID)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error opening %v", objectID)
	}
	defer r.Close() //nolint:errcheck

	indexEntries, totalLength = appendIndexEntries(indexEntries, startingLength, IndirectObjectEntry{
		Start:  0,
		Length: r.Length(),
		Object: objectID,
	})

	return indexEntries, totalLength, nil
}

func appendIndexEntries(indexEntries []IndirectObjectEntry, startingLength int64, incoming ...IndirectObjectEntry) (result []IndirectObjectEntry, totalLength int64) {
	totalLength = startingLength

	for _, inc := range incoming {
		indexEntries = append(indexEntries, IndirectObjectEntry{
			Start:  inc.Start + startingLength,
			Length: inc.Length,
			Object: inc.Object,
		})

		totalLength += inc.Length
	}

	return indexEntries, totalLength
}

func noop(content.ID) error { return nil }

// PrefetchBackingContents attempts to brings contents backing the provided object IDs into the cache.
// This may succeed only partially due to cache size limits and other.
// Returns the list of content IDs prefetched.
func PrefetchBackingContents(ctx context.Context, contentMgr contentManager, objectIDs []ID, hint string) ([]content.ID, error) {
	tracker := &contentIDTracker{}

	for _, oid := range objectIDs {
		if err := iterateBackingContents(ctx, contentMgr, oid, tracker, noop); err != nil && !errors.Is(err, ErrObjectNotFound) && !errors.Is(err, content.ErrContentNotFound) {
			return nil, err
		}
	}

	return contentMgr.PrefetchContents(ctx, tracker.contentIDs(), hint), nil
}

// NewObjectManager creates an ObjectManager with the specified content manager and format.
func NewObjectManager(ctx context.Context, bm contentManager, f format.ObjectFormat, mr *metrics.Registry) (*Manager, error) {
	_ = mr

	om := &Manager{
		contentMgr: bm,
		Format:     f,
	}

	om.writerPool = sync.Pool{
		New: func() interface{} {
			return new(objectWriter)
		},
	}

	splitterID := f.Splitter
	if splitterID == "" {
		splitterID = "FIXED"
	}

	os := splitter.GetFactory(splitterID)
	if os == nil {
		return nil, errors.Errorf("unsupported splitter %q", f.Splitter)
	}

	om.newDefaultSplitter = os

	return om, nil
}
