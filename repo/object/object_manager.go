// Package object implements repository support for content-addressable objects of arbitrary size.
package object

import (
	"bytes"
	"context"
	"encoding/json"
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

type contentManager interface {
	ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error)
	GetContent(ctx context.Context, contentID content.ID) ([]byte, error)
	WriteContent(ctx context.Context, data []byte, prefix content.ID) (content.ID, error)
}

// Format describes the format of objects in a repository.
type Format struct {
	Splitter string `json:"splitter,omitempty"` // splitter used to break objects into pieces of content
}

// Manager implements a content-addressable storage on top of blob storage.
type Manager struct {
	Format Format

	contentMgr contentManager
	trace      func(message string, args ...interface{})

	newSplitter splitter.Factory

	bufferPool *buf.Pool
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

// Open creates new ObjectReader for reading given object from a repository.
func (om *Manager) Open(ctx context.Context, objectID ID) (Reader, error) {
	return om.openAndAssertLength(ctx, objectID, -1)
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
		concatenatedEntries, totalLength, err = om.appendIndexEntriesForObject(ctx, concatenatedEntries, totalLength, objectID)
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

func (om *Manager) appendIndexEntriesForObject(ctx context.Context, indexEntries []indirectObjectEntry, startingLength int64, objectID ID) (result []indirectObjectEntry, totalLength int64, _ error) {
	if indexObjectID, ok := objectID.IndexObjectID(); ok {
		ndx, err := om.loadSeekTable(ctx, indexObjectID)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error reading index of %v", objectID)
		}

		indexEntries, totalLength = appendIndexEntries(indexEntries, startingLength, ndx...)

		return indexEntries, totalLength, nil
	}

	// non-index object - the precise length of the object cannot be determined from content due to compression and padding,
	// so we must open the object to read its length.
	r, err := om.Open(ctx, objectID)
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

func (om *Manager) openAndAssertLength(ctx context.Context, objectID ID, assertLength int64) (Reader, error) {
	if indexObjectID, ok := objectID.IndexObjectID(); ok {
		// recursively calls openAndAssertLength
		seekTable, err := om.loadSeekTable(ctx, indexObjectID)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			ctx:         ctx,
			repo:        om,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return om.newRawReader(ctx, objectID, assertLength)
}

// VerifyObject ensures that all objects backing ObjectID are present in the repository
// and returns the content IDs of which it is composed.
func (om *Manager) VerifyObject(ctx context.Context, oid ID) ([]content.ID, error) {
	tracker := &contentIDTracker{}

	if err := om.verifyObjectInternal(ctx, oid, tracker); err != nil {
		return nil, err
	}

	return tracker.contentIDs(), nil
}

func (om *Manager) verifyIndirectObjectInternal(ctx context.Context, indexObjectID ID, tracker *contentIDTracker) error {
	if err := om.verifyObjectInternal(ctx, indexObjectID, tracker); err != nil {
		return errors.Wrap(err, "unable to read index")
	}

	seekTable, err := om.loadSeekTable(ctx, indexObjectID)
	if err != nil {
		return err
	}

	for _, m := range seekTable {
		err := om.verifyObjectInternal(ctx, m.Object, tracker)
		if err != nil {
			return err
		}
	}

	return nil
}

func (om *Manager) verifyObjectInternal(ctx context.Context, oid ID, tracker *contentIDTracker) error {
	if indexObjectID, ok := oid.IndexObjectID(); ok {
		return om.verifyIndirectObjectInternal(ctx, indexObjectID, tracker)
	}

	if contentID, _, ok := oid.ContentID(); ok {
		if _, err := om.contentMgr.ContentInfo(ctx, contentID); err != nil {
			return errors.Wrapf(err, "error getting content info for %v", contentID)
		}

		tracker.addContentID(contentID)

		return nil
	}

	return errors.Errorf("unrecognized object type: %v", oid)
}

func nullTrace(message string, args ...interface{}) {
}

// ManagerOptions specifies object manager options.
type ManagerOptions struct {
	Trace func(message string, args ...interface{})
}

// NewObjectManager creates an ObjectManager with the specified content manager and format.
func NewObjectManager(ctx context.Context, bm contentManager, f Format, opts ManagerOptions) (*Manager, error) {
	om := &Manager{
		contentMgr: bm,
		Format:     f,
		trace:      nullTrace,
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

	if opts.Trace != nil {
		om.trace = opts.Trace
	} else {
		om.trace = nullTrace
	}

	return om, nil
}

// Close closes the object manager.
func (om *Manager) Close() error {
	om.bufferPool.Close()
	return nil
}

/*

{"stream":"kopia:indirect","entries":[
{"l":1698099,"o":"D13ea27f9ad891ad4a2edfa983906863d"},
{"s":1698099,"l":1302081,"o":"De8ca8327cd3af5f4edbd5ed1009c525e"},
{"s":3000180,"l":4352499,"o":"D6b6eb48ca5361d06d72fe193813e42e1"},
{"s":7352679,"l":1170821,"o":"Dd14653f76b63802ed48be64a0e67fea9"},

{"s":91094118,"l":1645153,"o":"Daa55df764d881a1daadb5ea9de17abbb"}
]}
*/

type indirectObject struct {
	StreamID string                `json:"stream"`
	Entries  []indirectObjectEntry `json:"entries"`
}

func (om *Manager) loadSeekTable(ctx context.Context, indexObjectID ID) ([]indirectObjectEntry, error) {
	r, err := om.openAndAssertLength(ctx, indexObjectID, -1)
	if err != nil {
		return nil, err
	}
	defer r.Close() //nolint:errcheck

	var ind indirectObject

	if err := json.NewDecoder(r).Decode(&ind); err != nil {
		return nil, errors.Wrap(err, "invalid indirect object")
	}

	return ind.Entries, nil
}

func (om *Manager) newRawReader(ctx context.Context, objectID ID, assertLength int64) (Reader, error) {
	contentID, compressed, ok := objectID.ContentID()
	if !ok {
		return nil, errors.Errorf("unsupported object ID: %v", objectID)
	}

	payload, err := om.contentMgr.GetContent(ctx, contentID)
	if errors.Is(err, content.ErrContentNotFound) {
		return nil, errors.Wrapf(ErrObjectNotFound, "content %v not found", contentID)
	}

	if err != nil {
		return nil, errors.Wrap(err, "unexpected content error")
	}

	if compressed {
		var b bytes.Buffer

		if err = om.decompress(&b, payload); err != nil {
			return nil, errors.Wrap(err, "decompression error")
		}

		payload = b.Bytes()
	}

	if assertLength != -1 && int64(len(payload)) != assertLength {
		return nil, errors.Wrapf(err, "unexpected chunk length %v, expected %v", len(payload), assertLength)
	}

	return newObjectReaderWithData(payload), nil
}

func (om *Manager) decompress(output *bytes.Buffer, b []byte) error {
	compressorID, err := compression.IDFromHeader(b)
	if err != nil {
		return errors.Wrap(err, "invalid compression header")
	}

	compressor := compression.ByHeaderID[compressorID]
	if compressor == nil {
		return errors.Errorf("unsupported compressor %x", compressorID)
	}

	return compressor.Decompress(output, b)
}

type readerWithData struct {
	io.ReadSeeker
	length int64
}

func (rwd *readerWithData) Close() error {
	return nil
}

func (rwd *readerWithData) Length() int64 {
	return rwd.length
}

func newObjectReaderWithData(data []byte) Reader {
	return &readerWithData{
		ReadSeeker: bytes.NewReader(data),
		length:     int64(len(data)),
	}
}
