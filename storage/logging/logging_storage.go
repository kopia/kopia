// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"log"
	"time"

	"github.com/kopia/kopia/storage"
)

type loggingStorage struct {
	base   storage.Storage
	printf func(string, ...interface{})
	prefix string
}

func (s *loggingStorage) BlockSize(id string) (int64, error) {
	t0 := time.Now()
	result, err := s.base.BlockSize(id)
	dt := time.Since(t0)
	s.printf(s.prefix+"BlockSize(%#v)=%#v,%#v took %v", id, result, err, dt)
	return result, err
}

func (s *loggingStorage) GetBlock(id string) ([]byte, error) {
	t0 := time.Now()
	result, err := s.base.GetBlock(id)
	dt := time.Since(t0)
	if len(result) < 20 {
		s.printf(s.prefix+"GetBlock(%#v)=(%#v, %#v) took %v", id, result, err, dt)
	} else {
		s.printf(s.prefix+"GetBlock(%#v)=({%#v bytes}, %#v) took %v", id, len(result), err, dt)
	}
	return result, err
}

func (s *loggingStorage) PutBlock(id string, data []byte, options storage.PutOptions) error {
	t0 := time.Now()
	err := s.base.PutBlock(id, data, options)
	dt := time.Since(t0)
	s.printf(s.prefix+"PutBlock(%#v, options=%v, len=%v)=%#v took %v", id, options, len(data), err, dt)
	return err
}

func (s *loggingStorage) DeleteBlock(id string) error {
	t0 := time.Now()
	err := s.base.DeleteBlock(id)
	dt := time.Since(t0)
	s.printf(s.prefix+"DeleteBlock(%#v)=%#v took %v", id, err, dt)
	return err
}

func (s *loggingStorage) ListBlocks(prefix string) chan (storage.BlockMetadata) {
	t0 := time.Now()
	ch := s.base.ListBlocks(prefix)
	dt := time.Since(t0)
	s.printf(s.prefix+"ListBlocks(%#v) took %v", prefix, dt)
	return ch
}

func (s *loggingStorage) Close() error {
	t0 := time.Now()
	err := s.base.Close()
	dt := time.Since(t0)
	s.printf(s.prefix+"Close()=%#v took %v", err, dt)
	return err
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
