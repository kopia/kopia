package repo

import (
	"io"
	"sync/atomic"
)

// countingReader wraps an io.ReadCloser and counts bytes read.
type countingReader struct {
	io.ReadCloser

	counter *int64
}

func (cr *countingReader) Read(b []byte) (int, error) {
	n, err := cr.ReadCloser.Read(b)
	atomic.AddInt64(cr.counter, int64(n))
	return n, err
}

func newCountingReader(source io.ReadCloser, counter *int64) io.ReadCloser {
	return &countingReader{
		ReadCloser: source,
		counter:    counter,
	}
}
