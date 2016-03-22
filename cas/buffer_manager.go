package cas

import (
	"bytes"
	"io"
	"log"
	"sync"
	"sync/atomic"
)

// bufferManager manages pool of reusable bytes.Buffer objects.
type bufferManager struct {
	outstandingCount int32

	pool sync.Pool
}

// newBuffer returns a new or reused bytes.Buffer.
func (mgr *bufferManager) newBuffer() *bytes.Buffer {
	atomic.AddInt32(&mgr.outstandingCount, 1)

	b := mgr.pool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

// returnBuffer returns the give buffer to the pool
func (mgr *bufferManager) returnBuffer(b *bytes.Buffer) {
	atomic.AddInt32(&mgr.outstandingCount, -1)
	mgr.pool.Put(b)
}

func (mgr *bufferManager) returnBufferOnClose(b *bytes.Buffer) io.ReadCloser {
	return &returnOnCloser{
		buffer: b,
		mgr:    mgr,
	}
}

func (mgr *bufferManager) close() {
	if mgr.outstandingCount != 0 {
		log.Println("WARNING: Found buffer leaks.")
	}
}

type returnOnCloser struct {
	buffer *bytes.Buffer
	mgr    *bufferManager
}

func (roc *returnOnCloser) Read(b []byte) (int, error) {
	return roc.buffer.Read(b)
}

func (roc *returnOnCloser) Close() error {
	roc.mgr.returnBuffer(roc.buffer)
	return nil
}

func newBufferManager(blockSize int) *bufferManager {
	mgr := &bufferManager{}
	mgr.pool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, blockSize))
		},
	}
	return mgr

}
