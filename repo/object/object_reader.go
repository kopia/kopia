package object

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
)

// Open creates new ObjectReader for reading given object from a repository.
func Open(ctx context.Context, r contentReader, objectID ID) (Reader, error) {
	return openAndAssertLength(ctx, r, objectID, -1)
}

// VerifyObject ensures that all objects backing ObjectID are present in the repository
// and returns the content IDs of which it is composed.
func VerifyObject(ctx context.Context, cr contentReader, oid ID) ([]content.ID, error) {
	tracker := &contentIDTracker{}

	if err := iterateBackingContents(ctx, cr, oid, tracker, func(contentID content.ID) error {
		if _, err := cr.ContentInfo(ctx, contentID); err != nil {
			return errors.Wrapf(err, "error getting content info for %v", contentID)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return tracker.contentIDs(), nil
}

type objectReader struct {
	// objectReader implements io.Reader, but needs context to read from repository
	ctx context.Context //nolint:containedctx

	cr contentReader

	seekTable []IndirectObjectEntry

	currentPosition int64 // Overall position in the objectReader
	totalLength     int64 // Overall length

	currentChunkIndex    int    // Index of current chunk in the seek table
	currentChunkData     []byte // Current chunk data
	currentChunkPosition int    // Read position in the current chunk
}

func (r *objectReader) Read(buffer []byte) (int, error) {
	readBytes := 0
	remaining := len(buffer)

	if r.currentPosition >= r.totalLength {
		return 0, io.EOF
	}

	for remaining > 0 {
		if r.currentChunkData != nil {
			toCopy := len(r.currentChunkData) - r.currentChunkPosition
			if toCopy == 0 {
				// EOF on current chunk
				r.closeCurrentChunk()

				r.currentChunkIndex++

				continue
			}

			if toCopy > remaining {
				toCopy = remaining
			}

			copy(buffer[readBytes:],
				r.currentChunkData[r.currentChunkPosition:r.currentChunkPosition+toCopy])

			r.currentChunkPosition += toCopy
			r.currentPosition += int64(toCopy)
			readBytes += toCopy
			remaining -= toCopy

			continue
		}

		if r.currentChunkIndex < len(r.seekTable) {
			err := r.openCurrentChunk()
			if err != nil {
				return 0, err
			}
		} else {
			break
		}
	}

	if readBytes == 0 {
		return readBytes, io.EOF
	}

	return readBytes, nil
}

func (r *objectReader) openCurrentChunk() error {
	st := r.seekTable[r.currentChunkIndex]

	rd, err := openAndAssertLength(r.ctx, r.cr, st.Object, st.Length)
	if err != nil {
		return err
	}

	defer rd.Close() //nolint:errcheck

	b := make([]byte, st.Length)
	if _, err := io.ReadFull(rd, b); err != nil {
		return errors.Wrap(err, "error reading chunk")
	}

	r.currentChunkData = b
	r.currentChunkPosition = 0

	return nil
}

func (r *objectReader) closeCurrentChunk() {
	r.currentChunkData = nil
}

func (r *objectReader) findChunkIndexForOffset(offset int64) (int, error) {
	left := 0
	right := len(r.seekTable) - 1

	for left <= right {
		middle := (left + right) / 2 //nolint:mnd

		if offset < r.seekTable[middle].Start {
			right = middle - 1
			continue
		}

		if offset >= r.seekTable[middle].endOffset() {
			left = middle + 1
			continue
		}

		return middle, nil
	}

	return 0, errors.Errorf("can't find chunk for offset %v", offset)
}

func (r *objectReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		return r.Seek(r.currentPosition+offset, 0)
	}

	if whence == io.SeekEnd {
		return r.Seek(r.totalLength+offset, 0)
	}

	if offset >= r.totalLength {
		r.currentChunkIndex = len(r.seekTable)
		r.currentChunkData = nil
		r.currentPosition = offset

		return offset, nil
	}

	index, err := r.findChunkIndexForOffset(offset)
	if err != nil {
		return -1, errors.Wrapf(err, "invalid seek %v %v", offset, whence)
	}

	chunkStartOffset := r.seekTable[index].Start

	if index != r.currentChunkIndex {
		r.closeCurrentChunk()
		r.currentChunkIndex = index
	}

	if r.currentChunkData == nil {
		if err := r.openCurrentChunk(); err != nil {
			return 0, err
		}
	}

	r.currentChunkPosition = int(offset - chunkStartOffset)
	r.currentPosition = offset

	return r.currentPosition, nil
}

func (r *objectReader) Close() error {
	return nil
}

func (r *objectReader) Length() int64 {
	return r.totalLength
}

func openAndAssertLength(ctx context.Context, cr contentReader, objectID ID, assertLength int64) (Reader, error) {
	if indexObjectID, ok := objectID.IndexObjectID(); ok {
		// recursively calls openAndAssertLength
		seekTable, err := LoadIndexObject(ctx, cr, indexObjectID)
		if err != nil {
			return nil, err
		}

		totalLength := seekTable[len(seekTable)-1].endOffset()

		return &objectReader{
			ctx:         ctx,
			cr:          cr,
			seekTable:   seekTable,
			totalLength: totalLength,
		}, nil
	}

	return newRawReader(ctx, cr, objectID, assertLength)
}

func iterateIndirectObjectContents(ctx context.Context, cr contentReader, indexObjectID ID, tracker *contentIDTracker, callbackFunc func(contentID content.ID) error) error {
	if err := iterateBackingContents(ctx, cr, indexObjectID, tracker, callbackFunc); err != nil {
		return errors.Wrap(err, "unable to read index")
	}

	seekTable, err := LoadIndexObject(ctx, cr, indexObjectID)
	if err != nil {
		return err
	}

	for _, m := range seekTable {
		err := iterateBackingContents(ctx, cr, m.Object, tracker, callbackFunc)
		if err != nil {
			return err
		}
	}

	return nil
}

func iterateBackingContents(ctx context.Context, r contentReader, oid ID, tracker *contentIDTracker, callbackFunc func(contentID content.ID) error) error {
	if indexObjectID, ok := oid.IndexObjectID(); ok {
		return iterateIndirectObjectContents(ctx, r, indexObjectID, tracker, callbackFunc)
	}

	if contentID, _, ok := oid.ContentID(); ok {
		if err := callbackFunc(contentID); err != nil {
			return err
		}

		tracker.addContentID(contentID)

		return nil
	}

	return errors.Errorf("unrecognized object type: %v", oid)
}

type indirectObject struct {
	StreamID string                `json:"stream"`
	Entries  []IndirectObjectEntry `json:"entries"`
}

// LoadIndexObject returns entries comprising index object.
func LoadIndexObject(ctx context.Context, cr contentReader, indexObjectID ID) ([]IndirectObjectEntry, error) {
	r, err := openAndAssertLength(ctx, cr, indexObjectID, -1)
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

func newRawReader(ctx context.Context, cr contentReader, objectID ID, assertLength int64) (Reader, error) {
	contentID, compressed, ok := objectID.ContentID()
	if !ok {
		return nil, errors.Errorf("unsupported object ID: %v", objectID)
	}

	payload, err := cr.GetContent(ctx, contentID)
	if errors.Is(err, content.ErrContentNotFound) {
		return nil, errors.Wrapf(ErrObjectNotFound, "content %v not found", contentID)
	}

	if err != nil {
		return nil, errors.Wrap(err, "unexpected content error")
	}

	if compressed {
		var b bytes.Buffer

		if err = compression.DecompressByHeader(&b, bytes.NewReader(payload)); err != nil {
			return nil, errors.Wrap(err, "decompression error")
		}

		payload = b.Bytes()
	}

	if assertLength != -1 && int64(len(payload)) != assertLength {
		return nil, errors.Errorf("unexpected chunk length %v, expected %v", len(payload), assertLength)
	}

	return newObjectReaderWithData(payload), nil
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
