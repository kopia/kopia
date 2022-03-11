// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"context"
	"sync/atomic"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

type loggingStorage struct {
	concurrency    int32
	maxConcurrency int32

	base   blob.Storage
	prefix string
	logger logging.Logger
}

func (s *loggingStorage) beginConcurrency() {
	v := atomic.AddInt32(&s.concurrency, 1)

	if mv := atomic.LoadInt32(&s.maxConcurrency); v > mv {
		if atomic.CompareAndSwapInt32(&s.maxConcurrency, mv, v) {
			s.logger.Debugw(s.prefix+"concurrency level reached",
				"maxConcurrency", mv)
		}
	}
}

func (s *loggingStorage) endConcurrency() {
	atomic.AddInt32(&s.concurrency, -1)
}

func (s *loggingStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	err := s.base.GetBlob(ctx, id, offset, length, output)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"GetBlob",
		"blobID", id,
		"offset", offset,
		"length", length,
		"outputLength", output.Length(),
		"error", err,
		"duration", dt,
	)

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	timer := timetrack.StartTimer()
	c, err := s.base.GetCapacity(ctx)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"GetCapacity",
		"sizeBytes", c.SizeB,
		"freeBytes", c.FreeB,
		"error", err,
		"duration", dt,
	)

	// nolint:wrapcheck
	return c, err
}

func (s *loggingStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	result, err := s.base.GetMetadata(ctx, id)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"GetMetadata",
		"blobID", id,
		"result", result,
		"error", err,
		"duration", dt,
	)

	// nolint:wrapcheck
	return result, err
}

func (s *loggingStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	err := s.base.PutBlob(ctx, id, data, opts)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"PutBlob",
		"blobID", id,
		"length", data.Length(),
		"error", err,
		"duration", dt,
	)

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	err := s.base.DeleteBlob(ctx, id)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"DeleteBlob",
		"blobID", id,
		"error", err,
		"duration", dt,
	)
	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	cnt := 0
	err := s.base.ListBlobs(ctx, prefix, func(bi blob.Metadata) error {
		cnt++
		return callback(bi)
	})
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"ListBlobs",
		"prefix", prefix,
		"resultCount", cnt,
		"error", err,
		"duration", dt,
	)

	// nolint:wrapcheck
	return err
}

func (s *loggingStorage) Close(ctx context.Context) error {
	timer := timetrack.StartTimer()
	err := s.base.Close(ctx)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"Close",
		"error", err,
		"duration", dt,
	)

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
	timer := timetrack.StartTimer()
	err := s.base.FlushCaches(ctx)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"FlushCaches",
		"error", err,
		"duration", dt,
	)

	// nolint:wrapcheck
	return err
}

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped blob.Storage, logger logging.Logger, prefix string) blob.Storage {
	return &loggingStorage{base: wrapped, logger: logger, prefix: prefix}
}
