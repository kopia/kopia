package gather

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGatherWriteBuffer(t *testing.T) {
	// reset for testing
	all := &chunkAllocator{
		chunkSize: 100,
	}

	w := NewWriteBuffer()
	w.alloc = all

	defer w.Close()

	w.Append([]byte("hello "))
	fmt.Fprintf(w, "world!")

	if got, want := w.ToByteSlice(), []byte("hello world!"); !bytes.Equal(got, want) {
		t.Errorf("invaldi bytes %v, want %v", string(got), string(want))
	}

	if got, want := len(w.inner.Slices), 1; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	w.Append(bytes.Repeat([]byte("x"), all.chunkSize))

	if got, want := w.Length(), all.chunkSize+12; got != want {
		t.Errorf("invalid length: %v, want %v", got, want)
	}

	// one more slice was allocated
	if got, want := len(w.inner.Slices), 2; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	// write to fill the remainder of 2nd slice
	w.Append(bytes.Repeat([]byte("x"), all.chunkSize-12))

	// still 2 slices
	if got, want := len(w.inner.Slices), 2; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	// one more byte allocates new slice
	w.Append([]byte("x"))

	// still 3 slices
	if got, want := len(w.inner.Slices), 3; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	var tmp WriteBuffer
	defer tmp.Close()

	require.NoError(t, w.AppendSectionTo(&tmp, 1, 5))
	require.Equal(t, []byte("ello "), tmp.ToByteSlice())

	w.Reset()
}

func TestGatherDefaultWriteBuffer(t *testing.T) {
	var w WriteBuffer

	// one more byte allocates new slice
	w.Append([]byte("x"))

	if got, want := len(w.inner.Slices), 1; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}
}

func TestGatherWriteBufferContig(t *testing.T) {
	var w WriteBuffer
	defer w.Close()

	// allocate more than contig allocator can provide
	theCap := maxContiguousAllocator.chunkSize + 10
	b := w.MakeContiguous(theCap)
	require.Len(t, b, theCap)
	require.Equal(t, theCap, cap(b))
}

func TestGatherWriteBufferAllocatorSelector(t *testing.T) {
	var w WriteBuffer
	defer w.Close()

	w.MakeContiguous(1)
	require.Equal(t, w.alloc, typicalContiguousAllocator)

	w.MakeContiguous(typicalContiguousAllocator.chunkSize)
	require.Equal(t, w.alloc, typicalContiguousAllocator)

	w.MakeContiguous(typicalContiguousAllocator.chunkSize + 1)
	require.Equal(t, w.alloc, maxContiguousAllocator)

	w.MakeContiguous(maxContiguousAllocator.chunkSize)
	require.Equal(t, w.alloc, maxContiguousAllocator)

	w.MakeContiguous(maxContiguousAllocator.chunkSize + 1)
	require.Nil(t, w.alloc)
}

func TestGatherWriteBufferMax(t *testing.T) {
	b := NewWriteBufferMaxContiguous()
	defer b.Close()

	// write 1Mx5 bytes
	for range 1000000 {
		b.Append([]byte("hello"))
	}

	// make sure we have 1 contiguous buffer
	require.Len(t, b.Bytes().Slices, 1)

	// write 10Mx5 bytes
	for range 10000000 {
		b.Append([]byte("hello"))
	}

	// 51M requires 4x16MB buffers
	require.Len(t, b.Bytes().Slices, 4)
}
