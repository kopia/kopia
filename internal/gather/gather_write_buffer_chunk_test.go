package gather

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/splitter"
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

func TestContigAllocatorChunkSize(t *testing.T) {
	// verify that contiguous allocator has chunk size big enough for all splitter results
	// + some minimal overhead.
	const maxOverhead = 128

	for _, s := range splitter.SupportedAlgorithms() {
		mss := splitter.GetFactory(s)().MaxSegmentSize()

		if got, want := maxContiguousAllocator.chunkSize, mss+maxOverhead; got < want {
			t.Errorf("contiguous allocator chunk size too small: %v, want %v ", got, want)
		}
	}
}

func TestTrackAllocation(t *testing.T) {
	old := trackChunkAllocations

	trackChunkAllocations = true
	defer func() {
		trackChunkAllocations = old
	}()

	var tmp WriteBuffer
	defer tmp.Close()

	var log bytes.Buffer

	ctx := logging.WithLogger(context.Background(), logging.ToWriter(&log))
	DumpStats(ctx)
	require.Contains(t, log.String(), `"chunksAlive": 0`)
	require.NotContains(t, log.String(), "leaked chunk")

	tmp.Append([]byte{1, 2, 3})

	log.Reset()
	DumpStats(ctx)
	require.Contains(t, log.String(), `"chunksAlive": 1`)
	require.Contains(t, log.String(), "leaked chunk")

	log.Reset()
	tmp.Close()
	DumpStats(ctx)
	require.Contains(t, log.String(), `"chunksAlive": 0`)
	require.NotContains(t, log.String(), "leaked chunk")
}
