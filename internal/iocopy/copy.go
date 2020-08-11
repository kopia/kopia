// Package iocopy is a wrapper around io.Copy() that recycles shared buffers.
package iocopy

import (
	"io"
	"sync"
)

const bufSize = 65536

var bufferPool = sync.Pool{
	New: func() interface{} {
		p := make([]byte, bufSize)

		return &p
	},
}

// Copy is equivalent to io.Copy().
func Copy(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := bufferPool.Get().(*[]byte)

	defer bufferPool.Put(bufPtr)

	return io.CopyBuffer(dst, src, *bufPtr)
}
