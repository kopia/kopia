// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"context"
	"time"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

const maxLoggedBlobLength = 20 // maximum length of the blob to log contents of

type loggingStorage struct {
	base   blob.Storage
	printf func(string, ...interface{})
	prefix string
}

func (s *loggingStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	t0 := time.Now() // nolint:forbidigo
	err := s.base.GetBlob(ctx, id, offset, length, output)
	dt := time.Since(t0) // nolint:forbidigo

	if output.Length() < maxLoggedBlobLength {
		s.printf(s.prefix+"GetBlob(%q,%v,%v)=(%v, %#v) took %v", id, offset, length, output, err, dt)
	} else {
		s.printf(s.prefix+"GetBlob(%q,%v,%v)=({%v bytes}, %#v) took %v", id, offset, length, output.Length(), err, dt)
	}

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	t0 := time.Now() // nolint:forbidigo
	result, err := s.base.GetMetadata(ctx, id)
	dt := time.Since(t0) // nolint:forbidigo

	s.printf(s.prefix+"GetMetadata(%q)=(%v, %#v) took %v", id, result, err, dt)

	// nolint:wrapcheck
	return result, err
}

func (s *loggingStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes) error {
	t0 := time.Now() // nolint:forbidigo
	err := s.base.PutBlob(ctx, id, data)
	dt := time.Since(t0) // nolint:forbidigo
	s.printf(s.prefix+"PutBlob(%q,len=%v)=%#v took %v", id, data.Length(), err, dt)

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) SetTime(ctx context.Context, id blob.ID, t time.Time) error {
	t0 := time.Now() // nolint:forbidigo
	err := s.base.SetTime(ctx, id, t)
	dt := time.Since(t0) // nolint:forbidigo
	s.printf(s.prefix+"SetTime(%q,%v)=%#v took %v", id, t, err, dt)

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	t0 := time.Now() // nolint:forbidigo
	err := s.base.DeleteBlob(ctx, id)
	dt := time.Since(t0) // nolint:forbidigo
	s.printf(s.prefix+"DeleteBlob(%q)=%#v took %v", id, err, dt)

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	t0 := time.Now() // nolint:forbidigo
	cnt := 0
	err := s.base.ListBlobs(ctx, prefix, func(bi blob.Metadata) error {
		cnt++
		return callback(bi)
	})
	s.printf(s.prefix+"ListBlobs(%q)=%v returned %v items and took %v", prefix, err, cnt, time.Since(t0)) // nolint:forbidigo

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) Close(ctx context.Context) error {
	t0 := time.Now() // nolint:forbidigo
	err := s.base.Close(ctx)
	dt := time.Since(t0) // nolint:forbidigo
	s.printf(s.prefix+"Close()=%#v took %v", err, dt)

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.base.ConnectionInfo()
}

func (s *loggingStorage) DisplayName() string {
	return s.base.DisplayName()
}

func (s *loggingStorage) FlushCaches(ctx context.Context) error {
	t0 := time.Now() // nolint:forbidigo
	err := s.base.FlushCaches(ctx)
	dt := time.Since(t0) // nolint:forbidigo
	s.printf(s.prefix+"FlushCaches()=%#v took %v", err, dt)

	// nolint:wrapcheck
	return err
}

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped blob.Storage, printf func(msg string, args ...interface{}), prefix string) blob.Storage {
	return &loggingStorage{base: wrapped, printf: printf, prefix: prefix}
}
