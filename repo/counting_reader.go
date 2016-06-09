package repo

import (
	"sync/atomic"

	"github.com/kopia/kopia/blob"
)

// countingReader wraps an io.ReadCloser and counts bytes read.
type countingReader struct {
	blob.ReaderWithLength

	counter *int64
}

func (cr *countingReader) Read(b []byte) (int, error) {
	n, err := cr.ReaderWithLength.Read(b)
	atomic.AddInt64(cr.counter, int64(n))
	return n, err
}

func newCountingReader(source blob.ReaderWithLength, counter *int64) blob.ReaderWithLength {
	return &countingReader{
		ReaderWithLength: source,
		counter:          counter,
	}
}
