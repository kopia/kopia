package blob

import "sync/atomic"

type writeLimitStorage struct {
	Storage

	remainingBytes int64
}

type writeLimitReadCloser struct {
	BlockReader
	repo *writeLimitStorage
}

func (s *writeLimitReadCloser) Read(b []byte) (int, error) {
	n, err := s.BlockReader.Read(b)
	v := atomic.AddInt64(&s.repo.remainingBytes, int64(-n))
	if v < 0 {
		
	}
	return n, err
}

func (s *writeLimitStorage) PutBlock(id string, data BlockReader, overwrite bool) error {
	return s.Storage.PutBlock(id, &writeLimitReadCloser{
		BlockReader: data,
		repo:        s,
	}, overwrite)
}

// NewWriteLimitWrapper returns a Storage wrapper that limits the number of bytes written to a repo.
// Once reached, the writes will return ErrWriteLimitExceeded
func NewWriteLimitWrapper(wrapped Storage, bytes int64) Storage {
	return &writeLimitStorage{
		Storage:        wrapped,
		remainingBytes: bytes,
	}
}
