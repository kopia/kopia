package gather

// WriteBuffer is a write buffer for content of unknown size that manages
// data in a series of byte slices of uniform size.
type WriteBuffer struct {
	Bytes
}

// Close releases all memory allocated by this buffer.
func (b *WriteBuffer) Close() {
	for _, s := range b.Slices {
		releaseChunk(s)
	}

	b.Slices = nil
}

// Reset resets buffer back to empty.
func (b *WriteBuffer) Reset() {
	for _, s := range b.Slices {
		releaseChunk(s)
	}

	b.Slices = nil
}

// Write implements io.Writer for appending to the buffer.
func (b *WriteBuffer) Write(data []byte) (n int, err error) {
	b.Append(data)
	return len(data), nil
}

// Append appends the specified slice of bytes to the buffer.
func (b *WriteBuffer) Append(data []byte) {
	if len(b.Slices) == 0 {
		b.sliceBuf[0] = allocChunk()
		b.Slices = b.sliceBuf[0:1]
	}

	for len(data) > 0 {
		ndx := len(b.Slices) - 1
		remaining := cap(b.Slices[ndx]) - len(b.Slices[ndx])

		if remaining == 0 {
			b.Slices = append(b.Slices, allocChunk())
			ndx = len(b.Slices) - 1
			remaining = cap(b.Slices[ndx]) - len(b.Slices[ndx])
		}

		chunkSize := remaining
		if chunkSize > len(data) {
			chunkSize = len(data)
		}

		b.Slices[ndx] = append(b.Slices[ndx], data[0:chunkSize]...)
		data = data[chunkSize:]
	}
}

// NewWriteBuffer creates new write buffer.
func NewWriteBuffer() *WriteBuffer {
	return &WriteBuffer{}
}
