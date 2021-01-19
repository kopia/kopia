// Package object implements repository support for content-addressable objects of arbitrary size.
package object

import (
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/buf"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/splitter"
)

// maxCompressionOverheadPerSegment is maximum overhead that compression can incur.
const maxCompressionOverheadPerSegment = 16384

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
}

type contentManager interface {
	contentReader
	WriteContent(ctx context.Context, data []byte, prefix content.ID) (content.ID, error)
}

// Format describes the format of objects in a repository.
type Format struct {
	Splitter string `json:"splitter,omitempty"` // splitter used to break objects into pieces of content
}

// Manager implements a content-addressable storage on top of blob storage.
type Manager struct {
	Format Format

	contentMgr  contentManager
	newSplitter splitter.Factory
	bufferPool  *buf.Pool
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (om *Manager) NewWriter(ctx context.Context, opt WriterOptions) Writer {
	w := &objectWriter{
		ctx:         ctx,
		om:          om,
		splitter:    om.newSplitter(),
		description: opt.Description,
		prefix:      opt.Prefix,
		compressor:  compression.ByName[opt.Compressor],
	}

	// point the slice at the embedded array, so that we avoid allocations most of the time
	w.indirectIndex = w.indirectIndexBuf[:0]

	if opt.AsyncWrites > 0 {
		w.asyncWritesSemaphore = make(chan struct{}, opt.AsyncWrites)
	}

	w.initBuffer()

	return w
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
// in parallel utilizing more CPU cores. Because some split points now start at fixed bounaries and not content-specific,
// this causes some slight loss of deduplication at concatenation points (typically 1-2 contents, usually <10MB),
// so this method should only be used for very large files where this overhead is relatively small.
func (om *Manager) Concatenate(ctx context.Context, objectIDs []ID) (ID, error) {
	if len(objectIDs) == 0 {
		return "", errors.Errorf("empty list of objects")
	}

	if len(objectIDs) == 1 {
		return objectIDs[0], nil
	}

	var (
		concatenatedEntries []indirectObjectEntry
		totalLength         int64
		err                 error
	)

	for _, objectID := range objectIDs {
		concatenatedEntries, totalLength, err = appendIndexEntriesForObject(ctx, om.contentMgr, concatenatedEntries, totalLength, objectID)
		if err != nil {
			return "", errors.Wrapf(err, "error appending %v", objectID)
		}
	}

	log(ctx).Debugf("concatenated: %v total: %v", concatenatedEntries, totalLength)

	w := om.NewWriter(ctx, WriterOptions{
		Prefix:      indirectContentPrefix,
		Description: "CONCATENATED INDEX",
	})
	defer w.Close() // nolint:errcheck

	if werr := writeIndirectObject(w, concatenatedEntries); werr != nil {
		return "", werr
	}

	concatID, err := w.Result()
	if err != nil {
		return "", errors.Wrap(err, "error writing concatenated index")
	}

	return IndirectObjectID(concatID), nil
}

func appendIndexEntriesForObject(ctx context.Context, cr contentReader, indexEntries []indirectObjectEntry, startingLength int64, objectID ID) (result []indirectObjectEntry, totalLength int64, _ error) {
	if indexObjectID, ok := objectID.IndexObjectID(); ok {
		ndx, err := loadSeekTable(ctx, cr, indexObjectID)
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

	indexEntries, totalLength = appendIndexEntries(indexEntries, startingLength, indirectObjectEntry{
		Start:  0,
		Length: r.Length(),
		Object: objectID,
	})

	return indexEntries, totalLength, nil
}

func appendIndexEntries(indexEntries []indirectObjectEntry, startingLength int64, incoming ...indirectObjectEntry) (result []indirectObjectEntry, totalLength int64) {
	totalLength = startingLength

	for _, inc := range incoming {
		indexEntries = append(indexEntries, indirectObjectEntry{
			Start:  inc.Start + startingLength,
			Length: inc.Length,
			Object: inc.Object,
		})

		totalLength += inc.Length
	}

	return indexEntries, totalLength
}

// NewObjectManager creates an ObjectManager with the specified content manager and format.
func NewObjectManager(ctx context.Context, bm contentManager, f Format) (*Manager, error) {
	om := &Manager{
		contentMgr: bm,
		Format:     f,
	}

	splitterID := f.Splitter
	if splitterID == "" {
		splitterID = "FIXED"
	}

	os := splitter.GetFactory(splitterID)
	if os == nil {
		return nil, errors.Errorf("unsupported splitter %q", f.Splitter)
	}

	om.newSplitter = splitter.Pooled(os)

	om.bufferPool = buf.NewPool(ctx, om.newSplitter().MaxSegmentSize()+maxCompressionOverheadPerSegment, "object-manager")

	return om, nil
}

// Close closes the object manager.
func (om *Manager) Close() error {
	om.bufferPool.Close()
	return nil
}
