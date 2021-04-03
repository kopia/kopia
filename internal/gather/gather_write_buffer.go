package gather

import "sync"

// WriteBuffer is a write buffer for content of unknown size that manages
// data in a series of byte slices of uniform size.
type WriteBuffer struct {
	mu    sync.Mutex
	inner Bytes
}

// Close releases all memory allocated by this buffer.
func (b *WriteBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, s := range b.inner.Slices {
		releaseChunk(s)
	}

	b.inner.Slices = nil
}

// Reset resets buffer back to empty.
func (b *WriteBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, s := range b.inner.Slices {
		releaseChunk(s)
	}

	b.inner.Slices = nil
}

// Write implements io.Writer for appending to the buffer.
func (b *WriteBuffer) Write(data []byte) (n int, err error) {
	b.Append(data)
	return len(data), nil
}

// AppendSectionTo appends the section of the buffer to the provided slice and returns it.
func (b *WriteBuffer) AppendSectionTo(output []byte, offset, size int) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner.AppendSectionTo(output, offset, size)
}

// Length returns the combined length of all slices.
func (b *WriteBuffer) Length() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner.Length()
}

// GetBytes appends all bytes to the provided slice and returns it.
func (b *WriteBuffer) GetBytes(output []byte) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner.GetBytes(output)
}

// Bytes returns inner gather.Bytes.
func (b *WriteBuffer) Bytes() Bytes {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.inner
}

// Append appends the specified slice of bytes to the buffer.
func (b *WriteBuffer) Append(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.inner.Slices) == 0 {
		b.inner.sliceBuf[0] = allocChunk()
		b.inner.Slices = b.inner.sliceBuf[0:1]
	}

	for len(data) > 0 {
		ndx := len(b.inner.Slices) - 1
		remaining := cap(b.inner.Slices[ndx]) - len(b.inner.Slices[ndx])

		if remaining == 0 {
			b.inner.Slices = append(b.inner.Slices, allocChunk())
			ndx = len(b.inner.Slices) - 1
			remaining = cap(b.inner.Slices[ndx]) - len(b.inner.Slices[ndx])
		}

		chunkSize := remaining
		if chunkSize > len(data) {
			chunkSize = len(data)
		}

		b.inner.Slices[ndx] = append(b.inner.Slices[ndx], data[0:chunkSize]...)
		data = data[chunkSize:]
	}
}

// NewWriteBuffer creates new write buffer.
func NewWriteBuffer() *WriteBuffer {
	return &WriteBuffer{}
}
