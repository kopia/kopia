package gather

import (
	"bytes"
	"testing"
)

func TestWriteBufferChunk(t *testing.T) {
	// reset for testing
	all := &chunkAllocator{
		chunkSize:       100,
		maxFreeListSize: 10,
	}

	// reset for testing
	chunk1 := all.allocChunk()
	_ = append(chunk1, []byte("chunk1")...)

	if got, want := len(chunk1), 0; got != want {
		t.Errorf("invalid chunk len: %v, want %v", got, want)
	}

	if got, want := cap(chunk1), all.chunkSize; got != want {
		t.Errorf("invalid chunk cap: %v, want %v", got, want)
	}

	if got, want := all.freeListHighWaterMark, 0; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	chunk2 := all.allocChunk()
	_ = append(chunk2, []byte("chunk2")...)

	if got, want := all.freeListHighWaterMark, 0; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	all.releaseChunk(chunk2)

	if got, want := all.freeListHighWaterMark, 1; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	all.releaseChunk(chunk1)

	if got, want := all.freeListHighWaterMark, 2; got != want {
		t.Errorf("unexpected high water mark %v, want %v", got, want)
	}

	// allocate chunk3 - make sure we got the same slice as chunk1 (LIFO)
	chunk3 := all.allocChunk()
	if got, want := chunk3[0:6], []byte("chunk1"); !bytes.Equal(got, want) {
		t.Errorf("got wrong chunk data %q, want %q", string(got), string(want))
	}

	// allocate chunk4 - make sure we got the same slice as chunk1 (LIFO)
	chunk4 := all.allocChunk()
	if got, want := chunk4[0:6], []byte("chunk2"); !bytes.Equal(got, want) {
		t.Errorf("got wrong chunk data %q, want %q", string(got), string(want))
	}
}
