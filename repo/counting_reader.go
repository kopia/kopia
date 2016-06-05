package repo

import (
	"sync/atomic"

	"github.com/kopia/kopia/blob"
)

// countingReader wraps an io.ReadCloser and counts bytes read.
type countingReader struct {
	blob.BlockReader

	counter *int64
}

func (cr *countingReader) Read(b []byte) (int, error) {
	n, err := cr.BlockReader.Read(b)
	atomic.AddInt64(cr.counter, int64(n))
	return n, err
}

func newCountingReader(source blob.BlockReader, counter *int64) blob.BlockReader {
	return &countingReader{
		BlockReader: source,
		counter:     counter,
	}
}
