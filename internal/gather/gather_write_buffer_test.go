package gather

import (
	"bytes"
	"fmt"
	"testing"
)

func TestGatherWriteBuffer(t *testing.T) {
	// reset for testing
	freeList = nil
	freeListHighWaterMark = 0

	w := NewWriteBuffer()
	defer w.Close()

	w.Append([]byte("hello "))
	fmt.Fprintf(w, "world!")

	if got, want := w.GetBytes(nil), []byte("hello world!"); !bytes.Equal(got, want) {
		t.Errorf("invaldi bytes %v, want %v", string(got), string(want))
	}

	if got, want := len(w.Slices), 1; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	w.Append(bytes.Repeat([]byte("x"), chunkSize))

	if got, want := w.Length(), chunkSize+12; got != want {
		t.Errorf("invalid length: %v, want %v", got, want)
	}

	// one more slice was allocated
	if got, want := len(w.Slices), 2; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	// write to fill the remainder of 2nd slice
	w.Append(bytes.Repeat([]byte("x"), chunkSize-12))

	// still 2 slices
	if got, want := len(w.Slices), 2; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	// one more byte allocates new slice
	w.Append([]byte("x"))

	// still 3 slices
	if got, want := len(w.Slices), 3; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}

	w.Reset()
}

func TestGatherDefaultWriteBuffer(t *testing.T) {
	var w WriteBuffer

	// one more byte allocates new slice
	w.Append([]byte("x"))

	if got, want := len(w.Slices), 1; got != want {
		t.Errorf("invalid number of slices %v, want %v", got, want)
	}
}
