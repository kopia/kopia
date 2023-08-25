// Package storagemetrics implements wrapper around Storage that adds metrics around all activity.
package storagemetrics

import (
	"context"
	"time"

	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
)

type blobMetrics struct {
	base blob.Storage

	downloadedBytesPartial *metrics.Counter
	downloadedBytesFull    *metrics.Counter
	uploadedBytes          *metrics.Counter
	listBlobItems          *metrics.Counter

	getBlobPartialDuration      *metrics.Distribution[time.Duration]
	getBlobFullDuration         *metrics.Distribution[time.Duration]
	putBlobDuration             *metrics.Distribution[time.Duration]
	getCapacityDuration         *metrics.Distribution[time.Duration]
	getMetadataDuration         *metrics.Distribution[time.Duration]
	deleteBlobDuration          *metrics.Distribution[time.Duration]
	extendBlobRetentionDuration *metrics.Distribution[time.Duration]
	listBlobsDuration           *metrics.Distribution[time.Duration]
	closeDuration               *metrics.Distribution[time.Duration]
	flushCachesDuration         *metrics.Distribution[time.Duration]

	getBlobErrors             *metrics.Counter
	getCapacityErrors         *metrics.Counter
	getMetadataErrors         *metrics.Counter
	putBlobErrors             *metrics.Counter
	deleteBlobErrors          *metrics.Counter
	extendBlobRetentionErrors *metrics.Counter
	listBlobsErrors           *metrics.Counter
	closeErrors               *metrics.Counter
	flushCachesErrors         *metrics.Counter
}

func (s *blobMetrics) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	timer := timetrack.StartTimer()
	err := s.base.GetBlob(ctx, id, offset, length, output)
	dt := timer.Elapsed()

	if length < 0 {
		s.downloadedBytesFull.Add(int64(output.Length()))
		s.getBlobFullDuration.Observe(dt)
	} else {
		s.downloadedBytesPartial.Add(int64(output.Length()))
		s.getBlobPartialDuration.Observe(dt)
	}

	if err != nil {
		s.getBlobErrors.Add(1)
	}

	//nolint:wrapcheck
	return err
}

func (s *blobMetrics) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	timer := timetrack.StartTimer()
	c, err := s.base.GetCapacity(ctx)
	dt := timer.Elapsed()

	s.getCapacityDuration.Observe(dt)

	if err != nil {
		s.getCapacityErrors.Add(1)
	}

	//nolint:wrapcheck
	return c, err
}

func (s *blobMetrics) IsReadOnly() bool {
	return s.base.IsReadOnly()
}

func (s *blobMetrics) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	timer := timetrack.StartTimer()
	result, err := s.base.GetMetadata(ctx, id)
	dt := timer.Elapsed()

	s.getMetadataDuration.Observe(dt)

	if err != nil {
		s.getMetadataErrors.Add(1)
	}

	//nolint:wrapcheck
	return result, err
}

func (s *blobMetrics) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	timer := timetrack.StartTimer()
	err := s.base.PutBlob(ctx, id, data, opts)
	dt := timer.Elapsed()

	s.putBlobDuration.Observe(dt)

	if err != nil {
		s.putBlobErrors.Add(1)
	} else {
		s.uploadedBytes.Add(int64(data.Length()))
	}

	//nolint:wrapcheck
	return err
}

func (s *blobMetrics) DeleteBlob(ctx context.Context, id blob.ID) error {
	timer := timetrack.StartTimer()
	err := s.base.DeleteBlob(ctx, id)
	dt := timer.Elapsed()

	s.deleteBlobDuration.Observe(dt)

	if err != nil {
		s.deleteBlobErrors.Add(1)
	}

	//nolint:wrapcheck
	return err
}

func (s *blobMetrics) ExtendBlobRetention(ctx context.Context, id blob.ID, opts blob.ExtendOptions) error {
	timer := timetrack.StartTimer()
	err := s.base.ExtendBlobRetention(ctx, id, opts)
	dt := timer.Elapsed()

	s.extendBlobRetentionDuration.Observe(dt)

	if err != nil {
		s.extendBlobRetentionErrors.Add(1)
	}

	//nolint:wrapcheck
	return err
}

func (s *blobMetrics) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	timer := timetrack.StartTimer()
	cnt := int64(0)
	err := s.base.ListBlobs(ctx, prefix, func(bi blob.Metadata) error {
		cnt++
		return callback(bi)
	})
	dt := timer.Elapsed()

	s.listBlobItems.Add(cnt)
	s.listBlobsDuration.Observe(dt)

	if err != nil {
		s.listBlobsErrors.Add(1)
	}

	//nolint:wrapcheck
	return err
}

func (s *blobMetrics) Close(ctx context.Context) error {
	timer := timetrack.StartTimer()
	err := s.base.Close(ctx)
	dt := timer.Elapsed()

	s.closeDuration.Observe(dt)

	if err != nil {
		s.closeErrors.Add(1)
	}

	//nolint:wrapcheck
	return err
}

func (s *blobMetrics) ConnectionInfo() blob.ConnectionInfo {
	return s.base.ConnectionInfo()
}

func (s *blobMetrics) DisplayName() string {
	return s.base.DisplayName()
}

func (s *blobMetrics) FlushCaches(ctx context.Context) error {
	timer := timetrack.StartTimer()
	err := s.base.FlushCaches(ctx)
	dt := timer.Elapsed()

	s.flushCachesDuration.Observe(dt)

	if err != nil {
		s.flushCachesErrors.Add(1)
	}

	//nolint:wrapcheck
	return err
}

// NewWrapper returns a Storage wrapper that logs all storage commands.
func NewWrapper(wrapped blob.Storage, mr *metrics.Registry) blob.Storage {
	durationSummaryForMethod := func(m string) *metrics.Distribution[time.Duration] {
		return mr.DurationDistribution(
			"blob_storage_latency",
			"Latency of blob storage operation by method",
			metrics.IOLatencyThresholds,
			map[string]string{"method": m},
		)
	}

	errorCounterForMethod := func(m string) *metrics.Counter {
		return mr.CounterInt64(
			"blob_errors",
			"Number of storage operation errors by method",
			map[string]string{"method": m},
		)
	}

	return &blobMetrics{
		base: wrapped,

		downloadedBytesPartial: mr.CounterInt64("blob_download_partial_blob_bytes", "Number of bytes downloaded as partial blobs", nil),
		downloadedBytesFull:    mr.CounterInt64("blob_download_full_blob_bytes", "Number of bytes downloaded as full blobs", nil),
		uploadedBytes:          mr.CounterInt64("blob_upload_bytes", "Number of bytes uploaded", nil),
		listBlobItems:          mr.CounterInt64("blob_list_items", "Number of list items returned", nil),

		getBlobPartialDuration: durationSummaryForMethod("GetBlob-partial"),
		getBlobFullDuration:    durationSummaryForMethod("GetBlob-full"),
		getCapacityDuration:    durationSummaryForMethod("GetCapacity"),
		getMetadataDuration:    durationSummaryForMethod("GetMetadata"),
		putBlobDuration:        durationSummaryForMethod("PutBlob"),
		deleteBlobDuration:     durationSummaryForMethod("DeleteBlob"),
		listBlobsDuration:      durationSummaryForMethod("ListBlobs"),
		closeDuration:          durationSummaryForMethod("Close"),
		flushCachesDuration:    durationSummaryForMethod("FlushCaches"),

		getBlobErrors:     errorCounterForMethod("GetBlob"),
		getCapacityErrors: errorCounterForMethod("GetCapacity"),
		getMetadataErrors: errorCounterForMethod("GetMetadata"),
		putBlobErrors:     errorCounterForMethod("PutBlob"),
		deleteBlobErrors:  errorCounterForMethod("DeleteBlob"),
		listBlobsErrors:   errorCounterForMethod("ListBlobs"),
		closeErrors:       errorCounterForMethod("Close"),
		flushCachesErrors: errorCounterForMethod("FlushCaches"),
	}
}
