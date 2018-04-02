// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"context"
	"io"
	"time"

	"github.com/kopia/kopia/storage"
	"github.com/rs/zerolog/log"
)

type loggingStorage struct {
	base   storage.Storage
	printf func(string, ...interface{})
	prefix string
}

func (s *loggingStorage) GetBlock(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	t0 := time.Now()
	result, err := s.base.GetBlock(ctx, id, offset, length)
	dt := time.Since(t0)
	if len(result) < 20 {
		s.printf(s.prefix+"GetBlock(%q,%v,%v)=(%#v, %#v) took %v", id, offset, length, result, err, dt)
	} else {
		s.printf(s.prefix+"GetBlock(%q,%v,%v)=({%#v bytes}, %#v) took %v", id, offset, length, len(result), err, dt)
	}
	return result, err
}

func (s *loggingStorage) PutBlock(ctx context.Context, id string, r io.Reader) error {
	t0 := time.Now()
	err := s.base.PutBlock(ctx, id, r)
	dt := time.Since(t0)
	s.printf(s.prefix+"PutBlock(%q)=%#v took %v", id, err, dt)
	return err
}

func (s *loggingStorage) DeleteBlock(ctx context.Context, id string) error {
	t0 := time.Now()
	err := s.base.DeleteBlock(ctx, id)
	dt := time.Since(t0)
	s.printf(s.prefix+"DeleteBlock(%q)=%#v took %v", id, err, dt)
	return err
}

func (s *loggingStorage) ListBlocks(ctx context.Context, prefix string) <-chan storage.BlockMetadata {
	t0 := time.Now()
	ch := s.base.ListBlocks(ctx, prefix)
	s.printf(s.prefix+"ListBlocks(%q) took %v", prefix, time.Since(t0))
	return ch
}

func (s *loggingStorage) Close(ctx context.Context) error {
	t0 := time.Now()
	err := s.base.Close(ctx)
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
