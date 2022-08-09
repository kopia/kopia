// Package iocopy is a wrapper around io.Copy() that recycles shared buffers.
package iocopy

import (
	"io"
	"sync"
)

// BufSize is the size (in bytes) of the shared copy buffers Kopia uses to copy data.
const BufSize = 65536

var (
	mu sync.Mutex //nolint:gochecknoglobals

	// +checklocks:mu
	buffers [][]byte //nolint:gochecknoglobals
)

// GetBuffer allocates new temporary buffer suitable for copying data.
func GetBuffer() []byte {
	mu.Lock()
	defer mu.Unlock()

	if len(buffers) == 0 {
		return make([]byte, BufSize)
	}

	var b []byte

	n := len(buffers) - 1
	b, buffers = buffers[n], buffers[0:n]

	return b
}

// ReleaseBuffer releases the buffer back to the pool.
func ReleaseBuffer(b []byte) {
	mu.Lock()
	defer mu.Unlock()

	buffers = append(buffers, b)
}

// Copy is equivalent to io.Copy().
func Copy(dst io.Writer, src io.Reader) (int64, error) {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.
	if wt, ok := src.(io.WriterTo); ok {
		//nolint:wrapcheck
		return wt.WriteTo(dst)
	}

	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rt, ok := dst.(io.ReaderFrom); ok {
		//nolint:wrapcheck
		return rt.ReadFrom(src)
	}

	buf := GetBuffer()
	defer ReleaseBuffer(buf)

	//nolint:wrapcheck
	return io.CopyBuffer(dst, src, buf)
}

// JustCopy is just like Copy() but does not return the number of bytes.
func JustCopy(dst io.Writer, src io.Reader) error {
	_, err := Copy(dst, src)

	return err
}
