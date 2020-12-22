// Package gather implements data structures storing binary data organized
// in a series of byte slices of fixed size that only gathered together by the user.
package gather

import (
	"bytes"
	"io"
)

// Bytes represents a sequence of bytes split into slices.
type Bytes struct {
	Slices [][]byte

	// for common case where there's one slice, store the slice itself here
	// to avoid allocation
	sliceBuf [1][]byte
}

// AppendSectionTo appends the section of the buffer to the provided slice and returns it.
func (b *Bytes) AppendSectionTo(output []byte, offset, size int) []byte {
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

	output = append(output, b.Slices[sliceNdx][offset:offset+firstChunkSize]...)
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

		output = append(output, s[0:l]...)
		size -= l
		sliceNdx++
	}

	return output
}

// Length returns the combined length of all slices.
func (b Bytes) Length() int {
	l := 0

	for _, data := range b.Slices {
		l += len(data)
	}

	return l
}

// Reader returns a reader for the data.
func (b Bytes) Reader() io.Reader {
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

// GetBytes appends all bytes to the provided slice and returns it.
func (b Bytes) GetBytes(output []byte) []byte {
	for _, v := range b.Slices {
		output = append(output, v...)
	}

	return output
}

// WriteTo writes contents to the specified writer and returns number of bytes written.
func (b Bytes) WriteTo(w io.Writer) (int64, error) {
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
