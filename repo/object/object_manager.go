// Package object implements repository support for content-addressable objects of arbitrary size.
package object

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
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

	newSplitter func() Splitter
}

// NewWriter creates an ObjectWriter for writing to the repository.
func (om *Manager) NewWriter(ctx context.Context, opt WriterOptions) Writer {
	return &objectWriter{
		ctx:         ctx,
		repo:        om,
		splitter:    om.newSplitter(),
		description: opt.Description,
		prefix:      opt.Prefix,
	}
}

// Open creates new ObjectReader for reading given object from a repository.
func (om *Manager) Open(ctx context.Context, objectID ID) (Reader, error) {
	// log.Printf("Repository::Open %v", objectID.String())
	// defer log.Printf("finished Repository::Open() %v", objectID.String())

	if indexObjectID, ok := objectID.IndexObjectID(); ok {
		rd, err := om.Open(ctx, indexObjectID)
		if err != nil {
			return nil, err
		}
		defer rd.Close() //nolint:errcheck

		seekTable, err := om.flattenListChunk(rd)
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

	return om.newRawReader(ctx, objectID)
}

// VerifyObject ensures that all objects backing ObjectID are present in the repository
// and returns the total length of the object and content IDs of which it is composed.
func (om *Manager) VerifyObject(ctx context.Context, oid ID) (int64, []content.ID, error) {
	tracker := &contentIDTracker{}
	l, err := om.verifyObjectInternal(ctx, oid, tracker)
	if err != nil {
		return 0, nil, err
	}

	return l, tracker.contentIDs(), nil
}

func (om *Manager) verifyIndirectObjectInternal(ctx context.Context, indexObjectID ID, tracker *contentIDTracker) (int64, error) {
	if _, err := om.verifyObjectInternal(ctx, indexObjectID, tracker); err != nil {
		return 0, errors.Wrap(err, "unable to read index")
	}
	rd, err := om.Open(ctx, indexObjectID)
	if err != nil {
		return 0, err
	}
	defer rd.Close() //nolint:errcheck

	seekTable, err := om.flattenListChunk(rd)
	if err != nil {
		return 0, err
	}

	for i, m := range seekTable {
		l, err := om.verifyObjectInternal(ctx, m.Object, tracker)
		if err != nil {
			return 0, err
		}

		if l != m.Length {
			return 0, errors.Errorf("unexpected length of part %#v of indirect object %q: %v %v, expected %v", i, indexObjectID, m.Object, l, m.Length)
		}
	}

	totalLength := seekTable[len(seekTable)-1].endOffset()
	return totalLength, nil
}

func (om *Manager) verifyObjectInternal(ctx context.Context, oid ID, tracker *contentIDTracker) (int64, error) {
	if indexObjectID, ok := oid.IndexObjectID(); ok {
		return om.verifyIndirectObjectInternal(ctx, indexObjectID, tracker)
	}

	if contentID, ok := oid.ContentID(); ok {
		p, err := om.contentMgr.ContentInfo(ctx, contentID)
		if err != nil {
			return 0, err
		}
		tracker.addContentID(contentID)
		return int64(p.Length), nil
	}

	return 0, errors.Errorf("unrecognized object type: %v", oid)

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

	os := GetSplitterFactory(splitterID)
	if os == nil {
		return nil, errors.Errorf("unsupported splitter %q", f.Splitter)
	}

	om.newSplitter = os

	if opts.Trace != nil {
		om.trace = opts.Trace
	} else {
		om.trace = nullTrace
	}

	return om, nil
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

func (om *Manager) flattenListChunk(rawReader io.Reader) ([]indirectObjectEntry, error) {
	var ind indirectObject

	if err := json.NewDecoder(rawReader).Decode(&ind); err != nil {
		return nil, errors.Wrap(err, "invalid indirect object")
	}

	return ind.Entries, nil
}

func (om *Manager) newRawReader(ctx context.Context, objectID ID) (Reader, error) {
	if contentID, ok := objectID.ContentID(); ok {
		payload, err := om.contentMgr.GetContent(ctx, contentID)
		if err == content.ErrContentNotFound {
			return nil, ErrObjectNotFound
		}
		if err != nil {
			return nil, errors.Wrap(err, "unexpected content error")
		}

		return newObjectReaderWithData(payload), nil
	}

	return nil, errors.Errorf("unsupported object ID: %v", objectID)
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
