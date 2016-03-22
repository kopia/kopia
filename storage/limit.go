package storage

import (
	"io"
	"sync/atomic"
)

type writeLimitRepository struct {
	Repository

	remainingBytes int64
}

type writeLimitReadCloser struct {
	io.ReadCloser
	repo *writeLimitRepository
}

func (s *writeLimitReadCloser) Read(b []byte) (int, error) {
	n, err := s.ReadCloser.Read(b)
	atomic.AddInt64(&s.repo.remainingBytes, int64(-n))
	return n, err
}

func (s *writeLimitRepository) PutBlock(id BlockID, data io.ReadCloser, options PutOptions) error {
	if !options.IgnoreLimits {
		if atomic.LoadInt64(&s.remainingBytes) <= 0 {
			return ErrWriteLimitExceeded
		}
	}

	return s.Repository.PutBlock(id, &writeLimitReadCloser{
		ReadCloser: data,
		repo:       s,
	}, options)
}

// NewWriteLimitWrapper returns a Repository wrapper that limits the number of bytes written to a cas.
// Once reached, the writes will return ErrWriteLimitExceeded
func NewWriteLimitWrapper(wrapped Repository, bytes int64) Repository {
	return &writeLimitRepository{
		Repository:     wrapped,
		remainingBytes: bytes,
	}
}
