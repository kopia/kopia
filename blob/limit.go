package blob

import (
	"io"
	"sync/atomic"
)

type writeLimitStorage struct {
	Storage

	remainingBytes int64
}

type writeLimitReadCloser struct {
	io.ReadCloser
	repo *writeLimitStorage
}

func (s *writeLimitReadCloser) Read(b []byte) (int, error) {
	n, err := s.ReadCloser.Read(b)
	atomic.AddInt64(&s.repo.remainingBytes, int64(-n))
	return n, err
}

func (s *writeLimitStorage) PutBlock(id string, data io.ReadCloser, options PutOptions) error {
	if !options.IgnoreLimits {
		if atomic.LoadInt64(&s.remainingBytes) <= 0 {
			return ErrWriteLimitExceeded
		}
	}

	return s.Storage.PutBlock(id, &writeLimitReadCloser{
		ReadCloser: data,
		repo:       s,
	}, options)
}

// NewWriteLimitWrapper returns a Storage wrapper that limits the number of bytes written to a repo.
// Once reached, the writes will return ErrWriteLimitExceeded
func NewWriteLimitWrapper(wrapped Storage, bytes int64) Storage {
	return &writeLimitStorage{
		Storage:        wrapped,
		remainingBytes: bytes,
	}
}
