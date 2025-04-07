package logging

import (
	"strconv"
	"sync"
	"time"
	"unsafe"
)

// Buffer is a specialized buffer that can be kept in a pool used
// for constructing logging messages without allocation.
type Buffer struct {
	buf      [1024]byte
	validLen int // valid length
}

//nolint:gochecknoglobals
var bufPool = &sync.Pool{
	New: func() interface{} {
		return &Buffer{}
	},
}

// GetBuffer gets a logging buffer.
func GetBuffer() *Buffer {
	//nolint:forcetypeassert
	return bufPool.Get().(*Buffer)
}

// Release releases logging buffer back to the pool.
func (b *Buffer) Release() {
	b.Reset()

	bufPool.Put(b)
}

// Reset resets logging buffer back to zero length.
func (b *Buffer) Reset() {
	b.validLen = 0
}

// AppendByte appends a single byte/character.
func (b *Buffer) AppendByte(val byte) *Buffer {
	if b.validLen < len(b.buf) {
		b.buf[b.validLen] = val
		b.validLen++
	}

	return b
}

// AppendString appends a string of characters.
func (b *Buffer) AppendString(val string) *Buffer {
	vl := len(val)

	if b.validLen+vl > len(b.buf) {
		vl = len(b.buf) - b.validLen
	}

	if vl > 0 {
		copy(b.buf[b.validLen:b.validLen+vl], val)
		b.validLen += vl
	}

	return b
}

// AppendTime appends a time representation.
func (b *Buffer) AppendTime(val time.Time, layout string) *Buffer {
	var buf [64]byte

	return b.AppendBytes(val.AppendFormat(buf[:0], layout))
}

// AppendBytes appends a slice of bytes.
func (b *Buffer) AppendBytes(val []byte) *Buffer {
	vl := len(val)

	if b.validLen+vl > len(b.buf) {
		vl = len(b.buf) - b.validLen
	}

	if vl > 0 {
		copy(b.buf[b.validLen:b.validLen+vl], val)
		b.validLen += vl
	}

	return b
}

// AppendBoolean appends boolean string ("true" or "false").
func (b *Buffer) AppendBoolean(val bool) *Buffer {
	if val {
		return b.AppendString("true")
	}

	return b.AppendString("false")
}

// AppendInt32 appends int32 value formatted as a decimal string.
func (b *Buffer) AppendInt32(val int32) *Buffer {
	return b.AppendInt(int64(val), 10) //nolint:mnd
}

// AppendInt64 appends int64 value formatted as a decimal string.
func (b *Buffer) AppendInt64(val int64) *Buffer {
	return b.AppendInt(val, 10) //nolint:mnd
}

// AppendInt appends integer value formatted as a string in a given base.
func (b *Buffer) AppendInt(val int64, base int) *Buffer {
	var buf [64]byte

	return b.AppendBytes(strconv.AppendInt(buf[:0], val, base))
}

// AppendUint32 appends uint32 value formatted as a decimal string.
func (b *Buffer) AppendUint32(val uint32) *Buffer {
	return b.AppendUint(uint64(val), 10) //nolint:mnd
}

// AppendUint64 appends uint64 value formatted as a decimal string.
func (b *Buffer) AppendUint64(val uint64) *Buffer {
	return b.AppendUint(val, 10) //nolint:mnd
}

// AppendUint appends unsigned integer value formatted as a string in a given base.
func (b *Buffer) AppendUint(val uint64, base int) *Buffer {
	var buf [64]byte

	return b.AppendBytes(strconv.AppendUint(buf[:0], val, base))
}

// String returns a string value of a buffer. The value is valud as long as
// string remains allocated and no Append*() methods have been called.
func (b *Buffer) String() string {
	if b.validLen == 0 {
		return ""
	}

	return unsafe.String(&b.buf[0], b.validLen) //nolint:gosec
}
