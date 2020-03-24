package gather

import (
	"bytes"
	"testing"
)

func TestWriteBufferChunk(t *testing.T) {
	// reset for testing
	freeList = nil
	freeListHighWaterMark = 0

	chunk1 := allocChunk()
	_ = append(chunk1, []byte("chunk1")...)

	if got, want := len(chunk1), 0; got != want {
		t.Errorf("invalid chunk len: %v, want %v", got, want)
	}

	if got, want := cap(chunk1), chunkSize; got != want {
		t.Errorf("invalid chunk cap: %v, want %v", got, want)
	}

	if got, want := freeListHighWaterMark, 0; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	chunk2 := allocChunk()
	_ = append(chunk2, []byte("chunk2")...)

	if got, want := freeListHighWaterMark, 0; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	releaseChunk(chunk2)

	if got, want := freeListHighWaterMark, 1; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	releaseChunk(chunk1)

	if got, want := freeListHighWaterMark, 2; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	// allocate chunk3 - make sure we got the same slice as chunk1 (LIFO)
	chunk3 := allocChunk()
	if got, want := chunk3[0:6], []byte("chunk1"); !bytes.Equal(got, want) {
		t.Errorf("got wrong chunk data %q, want %q", string(got), string(want))
	}

	// allocate chunk4 - make sure we got the same slice as chunk1 (LIFO)
	chunk4 := allocChunk()
	if got, want := chunk4[0:6], []byte("chunk2"); !bytes.Equal(got, want) {
		t.Errorf("got wrong chunk data %q, want %q", string(got), string(want))
	}
}
