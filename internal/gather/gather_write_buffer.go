package gather

import (
	"io"
	"sync"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("gather") // +checklocksignore

// WriteBuffer is a write buffer for content of unknown size that manages
// data in a series of byte slices of uniform size.
type WriteBuffer struct {
	alloc  *chunkAllocator
	mu     sync.Mutex
	inner  Bytes
	closed bool
}

// Close releases all memory allocated by this buffer.
func (b *WriteBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()
	b.releaseChunksLocked()
	b.inner.invalidate()

	b.closed = true
}

// MakeContiguous ensures the write buffer consists of exactly one contiguous single slice of the provided length
// and returns the slice.
func (b *WriteBuffer) MakeContiguous(length int) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()
	b.releaseChunksLocked()

	var v []byte

	switch {
	case length <= typicalContiguousAllocator.chunkSize:
		// most commonly used allocator for default chunk size with max 8MB
		b.alloc = typicalContiguousAllocator
		v = b.allocChunkLocked()[0:length]

	case length <= maxContiguousAllocator.chunkSize:
		b.alloc = maxContiguousAllocator
		v = b.allocChunkLocked()[0:length]

	default:
		v = make([]byte, length)
	}

	b.inner.slices = [][]byte{v}

	return v
}

// Reset resets buffer back to empty.
func (b *WriteBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()
	b.releaseChunksLocked()
}

func (b *WriteBuffer) releaseChunksLocked() {
	if b.alloc != nil {
		for _, s := range b.inner.slices {
			b.alloc.releaseChunk(s)
		}
	}

	b.alloc = nil

	// calling b.inner.invalidate() is innefective because it is reset below
	b.inner = Bytes{}
}

// Write implements io.Writer for appending to the buffer.
func (b *WriteBuffer) Write(data []byte) (n int, err error) {
	b.Append(data)
	return len(data), nil
}

// AppendSectionTo appends the section of the buffer to the provided writer.
func (b *WriteBuffer) AppendSectionTo(w io.Writer, offset, size int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()

	return b.inner.AppendSectionTo(w, offset, size)
}

// Length returns the combined length of all slices.
func (b *WriteBuffer) Length() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()

	return b.inner.Length()
}

// ToByteSlice returns contents as a newly-allocated byte slice.
func (b *WriteBuffer) ToByteSlice() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()

	return b.inner.ToByteSlice()
}

// Bytes returns inner gather.Bytes.
// Notice: Use with caution, the returned Bytes are not concurrency safe.
// A routine reading from the returned Bytes may or may not observe a
// concurrent modification in this WriteBuffer.
func (b *WriteBuffer) Bytes() Bytes {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()

	return b.inner
}

// Append appends the specified slice of bytes to the buffer.
func (b *WriteBuffer) Append(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkNotClosedLocked()
	b.inner.assertValid()

	if len(b.inner.slices) == 0 {
		b.inner.sliceBuf[0] = b.allocChunkLocked()
		b.inner.slices = b.inner.sliceBuf[0:1]
	}

	for len(data) > 0 {
		ndx := len(b.inner.slices) - 1
		remaining := cap(b.inner.slices[ndx]) - len(b.inner.slices[ndx])

		if remaining == 0 {
			b.inner.slices = append(b.inner.slices, b.allocChunkLocked())
			ndx = len(b.inner.slices) - 1
			remaining = cap(b.inner.slices[ndx]) - len(b.inner.slices[ndx])
		}

		chunkSize := min(remaining, len(data))

		b.inner.slices[ndx] = append(b.inner.slices[ndx], data[0:chunkSize]...)
		data = data[chunkSize:]
	}
}

func (b *WriteBuffer) checkNotClosedLocked() {
	if b.closed {
		// programming error, thus panic
		panic("WriteBuffer already closed")
	}
}

func (b *WriteBuffer) allocChunkLocked() []byte {
	if b.alloc == nil {
		b.alloc = defaultAllocator
	}

	return b.alloc.allocChunk()
}

// NewWriteBuffer creates new write buffer.
func NewWriteBuffer() *WriteBuffer {
	return &WriteBuffer{}
}

// NewWriteBufferMaxContiguous creates new write buffer that will allocate large chunks.
func NewWriteBufferMaxContiguous() *WriteBuffer {
	return &WriteBuffer{alloc: maxContiguousAllocator}
}
