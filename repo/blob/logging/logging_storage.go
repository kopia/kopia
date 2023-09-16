// Package logging implements wrapper around Storage that logs all activity.
package logging

import (
	"context"
	"errors"
	"sync/atomic"

	"go.opentelemetry.io/otel"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var tracer = otel.Tracer("BlobStorage")

type loggingStorage struct {
	concurrency    atomic.Int32
	maxConcurrency atomic.Int32

	base   blob.Storage
	prefix string
	logger logging.Logger
}

func (s *loggingStorage) beginConcurrency() {
	v := s.concurrency.Add(1)

	if mv := s.maxConcurrency.Load(); v > mv {
		if s.maxConcurrency.CompareAndSwap(mv, v) && v > 0 {
			s.logger.Debugw(s.prefix+"concurrency level reached",
				"maxConcurrency", v)
		}
	}
}

func (s *loggingStorage) endConcurrency() {
	s.concurrency.Add(-1)
}

func (s *loggingStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	ctx, span := tracer.Start(ctx, "GetBlob")
	defer span.End()

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
		"error", s.translateError(err),
		"duration", dt,
	)

	//nolint:wrapcheck
	return err
}

func (s *loggingStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	ctx, span := tracer.Start(ctx, "GetCapacity")
	defer span.End()

	timer := timetrack.StartTimer()
	c, err := s.base.GetCapacity(ctx)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"GetCapacity",
		"sizeBytes", c.SizeB,
		"freeBytes", c.FreeB,
		"error", s.translateError(err),
		"duration", dt,
	)

	//nolint:wrapcheck
	return c, err
}

func (s *loggingStorage) IsReadOnly() bool {
	return s.base.IsReadOnly()
}

func (s *loggingStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	ctx, span := tracer.Start(ctx, "GetMetadata")
	defer span.End()

	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	result, err := s.base.GetMetadata(ctx, id)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"GetMetadata",
		"blobID", id,
		"result", result,
		"error", s.translateError(err),
		"duration", dt,
	)

	//nolint:wrapcheck
	return result, err
}

func (s *loggingStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	ctx, span := tracer.Start(ctx, "PutBlob")
	defer span.End()

	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	err := s.base.PutBlob(ctx, id, data, opts)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"PutBlob",
		"blobID", id,
		"length", data.Length(),
		"error", s.translateError(err),
		"duration", dt,
	)

	//nolint:wrapcheck
	return err
}

func (s *loggingStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	ctx, span := tracer.Start(ctx, "DeleteBlob")
	defer span.End()

	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	err := s.base.DeleteBlob(ctx, id)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"DeleteBlob",
		"blobID", id,
		"error", s.translateError(err),
		"duration", dt,
	)
	//nolint:wrapcheck
	return err
}

func (s *loggingStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	ctx, span := tracer.Start(ctx, "ListBlobs")
	defer span.End()

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
		"error", s.translateError(err),
		"duration", dt,
	)

	//nolint:wrapcheck
	return err
}

func (s *loggingStorage) Close(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Close")
	defer span.End()

	timer := timetrack.StartTimer()
	err := s.base.Close(ctx)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"Close",
		"error", s.translateError(err),
		"duration", dt,
	)

	//nolint:wrapcheck
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
		"error", s.translateError(err),
		"duration", dt,
	)

	//nolint:wrapcheck
	return err
}

func (s *loggingStorage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	ctx, span := tracer.Start(ctx, "ExtendBlobRetention")
	defer span.End()

	s.beginConcurrency()
	defer s.endConcurrency()

	timer := timetrack.StartTimer()
	err := s.base.ExtendBlobRetention(ctx, b, opts)
	dt := timer.Elapsed()

	s.logger.Debugw(s.prefix+"ExtendBlobRetention",
		"blobID", b,
		"error", err,
		"duration", dt,
	)
	//nolint:wrapcheck
	return err
}

func (s *loggingStorage) translateError(err error) interface{} {
	if err == nil {
		return nil
	}

	if errors.Is(err, blob.ErrBlobNotFound) {
		return err.Error()
	}

	return err
}

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped blob.Storage, logger logging.Logger, prefix string) blob.Storage {
	return &loggingStorage{base: wrapped, logger: logger, prefix: prefix}
}
