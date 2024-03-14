// Package gather implements data structures storing binary data organized
// in a series of byte slices of fixed size that only gathered together by the user.
package gather

import (
	"bytes"
	"io"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	//nolint:gochecknoglobals
	invalidSliceBuf = []byte(uuid.NewString())

	ErrInvalidOffset = errors.Errorf("invalid offset")
)

// Bytes represents a sequence of bytes split into slices.
type Bytes struct {
	Slices [][]byte

	// for common case where there's one slice, store the slice itself here
	// to avoid allocation
	sliceBuf [1][]byte
}

func (b *Bytes) invalidate() {
	b.sliceBuf[0] = invalidSliceBuf
	b.Slices = nil
}

func (b *Bytes) assertValid() {
	if len(b.sliceBuf[0]) == len(invalidSliceBuf) && bytes.Equal(b.sliceBuf[0], invalidSliceBuf) {
		panic("gather.Bytes is invalid")
	}
}

// AppendSectionTo writes the section of the buffer to the provided writer.
func (b *Bytes) AppendSectionTo(w io.Writer, offset, size int) error {
	b.assertValid()

	if offset < 0 {
		return errors.Errorf("invalid offset")
	}

	// find the index of starting slice
	sliceNdx := -1

	for i, p := range b.Slices {
		if offset < len(p) {
			sliceNdx = i
			break
		}

		offset -= len(p)
	}

	// not found
	if sliceNdx == -1 {
		return nil
	}

	// first slice, possibly with offset zero
	var firstChunkSize int
	if offset+size <= len(b.Slices[sliceNdx]) {
		firstChunkSize = size
	} else {
		// slice shorter
		firstChunkSize = len(b.Slices[sliceNdx]) - offset
	}

	if _, err := w.Write(b.Slices[sliceNdx][offset : offset+firstChunkSize]); err != nil {
		return errors.Wrap(err, "error appending")
	}

	size -= firstChunkSize
	sliceNdx++

	// at this point we're staying at offset 0
	for size > 0 && sliceNdx < len(b.Slices) {
		s := b.Slices[sliceNdx]

		// l is how many bytes we consume out of the current slice
		l := size
		if l > len(s) {
			l = len(s)
		}

		if _, err := w.Write(s[0:l]); err != nil {
			return errors.Wrap(err, "error appending")
		}

		size -= l
		sliceNdx++
	}

	return nil
}

// Length returns the combined length of all slices.
func (b Bytes) Length() int {
	b.assertValid()

	l := 0

	for _, data := range b.Slices {
		l += len(data)
	}

	return l
}

// ReadAt implements io.ReaderAt interface.
func (b Bytes) ReadAt(p []byte, off int64) (n int, err error) {
	b.assertValid()

	return len(p), b.AppendSectionTo(bytes.NewBuffer(p[:0]), int(off), len(p))
}

type bytesReadSeekCloser struct {
	b      Bytes
	offset int
}

func (b *bytesReadSeekCloser) ReadAt(p []byte, off int64) (int, error) {
	b.b.assertValid()

	// source data that is read will be written to w, the buffer backed by p.
	offset := off

	plen := len(p)

	// negative offsets result in an error
	if offset < 0 {
		return 0, ErrInvalidOffset
	}

	sliceNdx := -1

	// find the index of starting slice
	for i, p := range b.b.Slices {
		if offset < int64(len(p)) {
			sliceNdx = i
			break
		}

		// update offset to be relative to the sliceNdx slice
		offset -= int64(len(p))
	}

	// no slice found if sliceNdx is still negative
	if sliceNdx == -1 {
		// return no bytes read if the buffer has no length
		if plen == 0 {
			return 0, nil
		}

		return 0, io.EOF
	}

	s := b.b.Slices[sliceNdx]

	n := copy(p, s[offset:])

	// first chunk written, move on to the next
	sliceNdx++

	// at this point we're staying at offset 0
	blen := len(b.b.Slices)
	// while there are bytes to read (plen) and not at the last slice in the
	// slice index
	m := 0

	for plen-n != 0 && sliceNdx < blen {
		s = b.b.Slices[sliceNdx]
		m = copy(p[n:], s)
		// accounting: keep track of total number of bytes written and
		// number of bytes written from the current slice
		n += m

		sliceNdx++
	}

	// if the slice index, sliceNdx is the last slice index in b and
	// the offset plus all the bytes read
	if sliceNdx == blen && m == len(s) {
		return n, io.EOF
	}

	return n, nil
}

func (b *bytesReadSeekCloser) Close() error {
	return nil
}

func (b *bytesReadSeekCloser) Read(buf []byte) (int, error) {
	l := len(buf)
	if b.offset+l > b.b.Length() {
		l = b.b.Length() - b.offset

		if l == 0 {
			return 0, io.EOF
		}
	}

	n, err := b.b.ReadAt(buf[0:l], int64(b.offset))
	b.offset += n

	return n, err
}

func (b *bytesReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	newOffset := b.offset

	switch whence {
	case io.SeekStart:
		newOffset = int(offset)
	case io.SeekCurrent:
		newOffset += int(offset)
	case io.SeekEnd:
		newOffset = b.b.Length() + int(offset)
	}

	if newOffset < 0 || newOffset > b.b.Length() {
		return 0, errors.Errorf("invalid seek")
	}

	b.offset = newOffset

	return int64(newOffset), nil
}

// Reader returns a reader for the data.
func (b Bytes) Reader() io.ReadSeekCloser {
	b.assertValid()

	return &bytesReadSeekCloser{b: b}
}

// AppendToSlice appends the contents to the provided slice.
func (b Bytes) AppendToSlice(output []byte) []byte {
	b.assertValid()

	for _, v := range b.Slices {
		output = append(output, v...)
	}

	return output
}

// ToByteSlice returns contents as a newly-allocated byte slice.
func (b Bytes) ToByteSlice() []byte {
	b.assertValid()

	return b.AppendToSlice(make([]byte, 0, b.Length()))
}

// WriteTo writes contents to the specified writer and returns number of bytes written.
func (b Bytes) WriteTo(w io.Writer) (int64, error) {
	b.assertValid()

	var totalN int64

	for _, v := range b.Slices {
		n, err := w.Write(v)

		totalN += int64(n)

		if err != nil {
			//nolint:wrapcheck
			return totalN, err
		}
	}

	return totalN, nil
}

// FromSlice creates Bytes from the specified slice.
func FromSlice(b []byte) Bytes {
	var r Bytes

	r.sliceBuf[0] = b
	r.Slices = r.sliceBuf[:]

	return r
}
