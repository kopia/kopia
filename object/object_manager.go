// Package object implements repository support for content-addressable objects of arbitrary size.
package object

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/kopia/repo/block"
	"github.com/pkg/errors"
)

// Reader allows reading, seeking, getting the length of and closing of a repository object.
type Reader interface {
	io.Reader
	io.Seeker
	io.Closer
	Length() int64
}

type blockManager interface {
	BlockInfo(ctx context.Context, blockID string) (block.Info, error)
	GetBlock(ctx context.Context, blockID string) ([]byte, error)
	WriteBlock(ctx context.Context, data []byte, prefix string) (string, error)
}

// Format describes the format of objects in a repository.
type Format struct {
	Splitter     string `json:"splitter,omitempty"`     // splitter used to break objects into storage blocks
	MinBlockSize int    `json:"minBlockSize,omitempty"` // minimum block size used with dynamic splitter
	AvgBlockSize int    `json:"avgBlockSize,omitempty"` // approximate size of storage block (used with dynamic splitter)
	MaxBlockSize int    `json:"maxBlockSize,omitempty"` // maximum size of storage block
}

// Manager implements a content-addressable storage on top of blob storage.
type Manager struct {
	Format Format

	blockMgr blockManager
	trace    func(message string, args ...interface{})

	newSplitter func() objectSplitter
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
// and returns the total length of the object and storage blocks of which it is composed.
func (om *Manager) VerifyObject(ctx context.Context, oid ID) (int64, []string, error) {
	blocks := &blockTracker{}
	l, err := om.verifyObjectInternal(ctx, oid, blocks)
	if err != nil {
		return 0, nil, err
	}

	return l, blocks.blockIDs(), nil
}

func (om *Manager) verifyIndirectObjectInternal(ctx context.Context, indexObjectID ID, blocks *blockTracker) (int64, error) {
	if _, err := om.verifyObjectInternal(ctx, indexObjectID, blocks); err != nil {
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
		l, err := om.verifyObjectInternal(ctx, m.Object, blocks)
		if err != nil {
			return 0, err
		}

		if l != m.Length {
			return 0, fmt.Errorf("unexpected length of part %#v of indirect object %q: %v %v, expected %v", i, indexObjectID, m.Object, l, m.Length)
		}
	}

	totalLength := seekTable[len(seekTable)-1].endOffset()
	return totalLength, nil
}

func (om *Manager) verifyObjectInternal(ctx context.Context, oid ID, blocks *blockTracker) (int64, error) {
	if indexObjectID, ok := oid.IndexObjectID(); ok {
		return om.verifyIndirectObjectInternal(ctx, indexObjectID, blocks)
	}

	if blockID, ok := oid.BlockID(); ok {
		p, err := om.blockMgr.BlockInfo(ctx, blockID)
		if err != nil {
			return 0, err
		}
		blocks.addBlock(blockID)
		return int64(p.Length), nil
	}

	return 0, fmt.Errorf("unrecognized object type: %v", oid)

}

func nullTrace(message string, args ...interface{}) {
}

// ManagerOptions specifies object manager options.
type ManagerOptions struct {
	Trace func(message string, args ...interface{})
}

// NewObjectManager creates an ObjectManager with the specified block manager and format.
func NewObjectManager(ctx context.Context, bm blockManager, f Format, opts ManagerOptions) (*Manager, error) {
	om := &Manager{
		blockMgr: bm,
		Format:   f,
		trace:    nullTrace,
	}

	splitterID := f.Splitter
	if splitterID == "" {
		splitterID = "FIXED"
	}

	os := splitterFactories[splitterID]
	if os == nil {
		return nil, fmt.Errorf("unsupported splitter %q", f.Splitter)
	}

	om.newSplitter = func() objectSplitter {
		return os(&f)
	}

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
	if blockID, ok := objectID.BlockID(); ok {
		payload, err := om.blockMgr.GetBlock(ctx, blockID)
		if err != nil {
			return nil, err
		}

		return newObjectReaderWithData(payload), nil
	}

	return nil, fmt.Errorf("unsupported object ID: %v", objectID)
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
