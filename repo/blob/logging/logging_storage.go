// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"context"
	"time"

	"github.com/kopia/kopia/internal/repologging"
	"github.com/kopia/kopia/repo/blob"
)

var log = repologging.Logger("repo/blob")

type loggingStorage struct {
	base   blob.Storage
	printf func(string, ...interface{})
	prefix string
}

func (s *loggingStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64) ([]byte, error) {
	t0 := time.Now()
	result, err := s.base.GetBlob(ctx, id, offset, length)
	dt := time.Since(t0)
	if len(result) < 20 {
		s.printf(s.prefix+"GetBlob(%q,%v,%v)=(%#v, %#v) took %v", id, offset, length, result, err, dt)
	} else {
		s.printf(s.prefix+"GetBlob(%q,%v,%v)=({%#v bytes}, %#v) took %v", id, offset, length, len(result), err, dt)
	}
	return result, err
}

func (s *loggingStorage) PutBlob(ctx context.Context, id blob.ID, data []byte) error {
	t0 := time.Now()
	err := s.base.PutBlob(ctx, id, data)
	dt := time.Since(t0)
	s.printf(s.prefix+"PutBlob(%q,len=%v)=%#v took %v", id, len(data), err, dt)
	return err
}

func (s *loggingStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	t0 := time.Now()
	err := s.base.DeleteBlob(ctx, id)
	dt := time.Since(t0)
	s.printf(s.prefix+"DeleteBlob(%q)=%#v took %v", id, err, dt)
	return err
}

func (s *loggingStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	t0 := time.Now()
	cnt := 0
	err := s.base.ListBlobs(ctx, prefix, func(bi blob.Metadata) error {
		cnt++
		return callback(bi)
	})
	s.printf(s.prefix+"ListBlobs(%q)=%v returned %v items and took %v", prefix, err, cnt, time.Since(t0))
	return err
}

func (s *loggingStorage) Close(ctx context.Context) error {
	t0 := time.Now()
	err := s.base.Close(ctx)
	dt := time.Since(t0)
	s.printf(s.prefix+"Close()=%#v took %v", err, dt)
	return err
}

func (s *loggingStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.base.ConnectionInfo()
}

// Option modifies the behavior of logging storage wrapper.
type Option func(s *loggingStorage)

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped blob.Storage, options ...Option) blob.Storage {
	s := &loggingStorage{base: wrapped, printf: log.Debugf}
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
