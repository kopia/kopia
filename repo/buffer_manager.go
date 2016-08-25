package repo

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

var panicOnBufferLeaks = false

// bufferManager manages pool of reusable bytes.Buffer objects.
type bufferManager struct {
	outstandingCount int32

	pool sync.Pool
}

// newBuffer returns a new or reused bytes.Buffer.
func (mgr *bufferManager) newBuffer() *bytes.Buffer {
	atomic.AddInt32(&mgr.outstandingCount, 1)
	//debug.PrintStack()
	//b := mgr.pool.Get().(*bytes.Buffer)
	b := mgr.pool.New().(*bytes.Buffer)
	b.Reset()
	return b
}

// returnBuffer returns the give buffer to the pool
func (mgr *bufferManager) returnBuffer(b *bytes.Buffer) {
	//log.Printf("returning buffer")
	atomic.AddInt32(&mgr.outstandingCount, -1)
	mgr.pool.Put(b)
}

func (mgr *bufferManager) close() {
	if mgr.outstandingCount != 0 {
		if panicOnBufferLeaks {
			log.Panicf("WARNING: Found %v buffer leaks.", mgr.outstandingCount)
		} else {
			log.Printf("WARNING: Found %v buffer leaks.", mgr.outstandingCount)
		}
	}
}

func newBufferManager(blockSize int) *bufferManager {
	mgr := &bufferManager{}
	mgr.pool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, blockSize))
		},
	}
	return mgr

}
