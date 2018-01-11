// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"time"

	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"
)

type loggingStorage struct {
	base   storage.Storage
	printf func(string, ...interface{})
	prefix string
}

func (s *loggingStorage) GetBlock(id string, offset, length int64) ([]byte, error) {
	t0 := time.Now()
	result, err := s.base.GetBlock(id, offset, length)
	dt := time.Since(t0)
	if len(result) < 20 {
		s.printf(s.prefix+"GetBlock(%q,%v,%v)=(%#v, %#v) took %v", id, offset, length, result, err, dt)
	} else {
		s.printf(s.prefix+"GetBlock(%q,%v,%v)=({%#v bytes}, %#v) took %v", id, offset, length, len(result), err, dt)
	}
	return result, err
}

func (s *loggingStorage) PutBlock(id string, data []byte) error {
	t0 := time.Now()
	err := s.base.PutBlock(id, data)
	dt := time.Since(t0)
	s.printf(s.prefix+"PutBlock(%q, len=%v)=%#v took %v", id, len(data), err, dt)
	return err
}

func (s *loggingStorage) DeleteBlock(id string) error {
	t0 := time.Now()
	err := s.base.DeleteBlock(id)
	dt := time.Since(t0)
	s.printf(s.prefix+"DeleteBlock(%q)=%#v took %v", id, err, dt)
	return err
}

func (s *loggingStorage) ListBlocks(prefix string) (<-chan storage.BlockMetadata, storage.CancelFunc) {
	t0 := time.Now()
	ch, cf := s.base.ListBlocks(prefix)
	s.printf(s.prefix+"ListBlocks(%q) took %v", prefix, time.Since(t0))
	return ch, func() {
		s.printf(s.prefix+"Cancelled ListBlocks(%q)after %v", prefix, time.Since(t0))
		cf()
	}
}

func (s *loggingStorage) Close() error {
	t0 := time.Now()
	err := s.base.Close()
	dt := time.Since(t0)
	s.printf(s.prefix+"Close()=%#v took %v", err, dt)
	return err
}

func (s *loggingStorage) ConnectionInfo() storage.ConnectionInfo {
	return s.base.ConnectionInfo()
}

// Option modifies the behavior of logging storage wrapper.
type Option func(s *loggingStorage)

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped storage.Storage, options ...Option) storage.Storage {
	s := &loggingStorage{base: wrapped, printf: log.Printf}
	for _, o := range options {
		o(s)
	}

	return s
}

// Output is a logging storage option that causes all output to be sent to a given function instead of log.Printf()
func Output(outputFunc func(fmt string, args ...interface{})) Option {
	return func(s *loggingStorage) {
		s.printf = outputFunc
	}
}

// Prefix specifies prefix to be prepended to all log output.
func Prefix(prefix string) Option {
	return func(s *loggingStorage) {
		s.prefix = prefix
	}
}
