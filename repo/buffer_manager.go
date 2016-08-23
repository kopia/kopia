package repo

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/kopia/kopia/storage"
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
	//log.Printf("getting buffer")

	//debug.PrintStack()
	b := mgr.pool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

// returnBuffer returns the give buffer to the pool
func (mgr *bufferManager) returnBuffer(b *bytes.Buffer) {
	//log.Printf("returning buffer")
	atomic.AddInt32(&mgr.outstandingCount, -1)
	mgr.pool.Put(b)
}

func (mgr *bufferManager) returnBufferOnClose(b *bytes.Buffer) storage.ReaderWithLength {
	return &returnOnCloser{
		buffer: b,
		mgr:    mgr,
	}
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

func (roc *returnOnCloser) Len() int {
	return roc.buffer.Len()
}

func (roc *returnOnCloser) String() string {
	return fmt.Sprintf("returnOnClose(len=%v)", roc.Len())
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
