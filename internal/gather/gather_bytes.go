// Package gather implements data structures storing binary data organized
// in a series of byte slices of fixed size that only gathered together by the user.
package gather

import (
	"bytes"
	"io"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var invalidSliceBuf = []byte(uuid.NewString())

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

// Reader returns a reader for the data.
func (b Bytes) Reader() io.Reader {
	b.assertValid()

	switch len(b.Slices) {
	case 0:
		return bytes.NewReader(nil)

	case 1:
		return bytes.NewReader(b.Slices[0])

	default:
		readers := make([]io.Reader, 0, len(b.Slices))

		for _, v := range b.Slices {
			readers = append(readers, bytes.NewReader(v))
		}

		return io.MultiReader(readers...)
	}
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
			// nolint:wrapcheck
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
