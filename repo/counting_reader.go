package repo

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/kopia/kopia/storage"
)

// countingReader wraps an io.ReadCloser and counts bytes read.
type countingReader struct {
	storage.ReaderWithLength

	counter *int64
}

func (cr *countingReader) Read(b []byte) (int, error) {
	n, err := cr.ReaderWithLength.Read(b)
	atomic.AddInt64(cr.counter, int64(n))
	return n, err
}

func (cr *countingReader) String() string {
	return fmt.Sprintf("countingReader(%v)", cr.ReaderWithLength)
}

func newCountingReader(source storage.ReaderWithLength, counter *int64) storage.ReaderWithLength {
	if uintptr(unsafe.Pointer(counter))&7 != 0 {
		panic("counter address must be 64-bit aligned")
	}

	return &countingReader{
		ReaderWithLength: source,
		counter:          counter,
	}
}
