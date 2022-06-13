package throttling

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// SettableThrottler exposes methods to set throttling limits.
type SettableThrottler interface {
	Throttler

	Limits() Limits
	SetLimits(limits Limits) error
	OnUpdate(handler UpdatedHandler)
}

// UpdatedHandler is invoked as part of SetLimits() after limits are updated.
type UpdatedHandler func(l Limits) error

type tokenBucketBasedThrottler struct {
	mu sync.Mutex
	// +checklocks:mu
	limits Limits

	readOps  *tokenBucket
	writeOps *tokenBucket
	listOps  *tokenBucket
	upload   *tokenBucket
	download *tokenBucket

	concurrentReads  *semaphore
	concurrentWrites *semaphore

	window time.Duration // +checklocksignore

	onUpdate []UpdatedHandler
}

func (t *tokenBucketBasedThrottler) BeforeOperation(ctx context.Context, op string) {
	switch op {
	case operationListBlobs:
		t.listOps.Take(ctx, 1)
	case operationGetBlob, operationGetMetadata:
		t.readOps.Take(ctx, 1)
		t.concurrentReads.Acquire()
	case operationPutBlob, operationDeleteBlob:
		t.writeOps.Take(ctx, 1)
		t.concurrentWrites.Acquire()
	}
}

func (t *tokenBucketBasedThrottler) AfterOperation(ctx context.Context, op string) {
	switch op {
	case operationListBlobs:
	case operationGetBlob, operationGetMetadata:
		t.concurrentReads.Release()
	case operationPutBlob, operationDeleteBlob:
		t.concurrentWrites.Release()
	}
}

func (t *tokenBucketBasedThrottler) BeforeDownload(ctx context.Context, numBytes int64) {
	t.download.Take(ctx, float64(numBytes))
}

func (t *tokenBucketBasedThrottler) ReturnUnusedDownloadBytes(ctx context.Context, numBytes int64) {
	t.download.Return(ctx, float64(numBytes))
}

func (t *tokenBucketBasedThrottler) BeforeUpload(ctx context.Context, numBytes int64) {
	t.upload.Take(ctx, float64(numBytes))
}

func (t *tokenBucketBasedThrottler) Limits() Limits {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.limits
}

// SetLimits overrides limits.
func (t *tokenBucketBasedThrottler) SetLimits(limits Limits) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.setLimits(limits); err != nil {
		_ = t.setLimits(t.limits)
		return err
	}

	t.limits = limits

	for _, h := range t.onUpdate {
		if err := h(limits); err != nil {
			return err
		}
	}

	return nil
}

func (t *tokenBucketBasedThrottler) setLimits(limits Limits) error {
	if err := t.readOps.SetLimit(limits.ReadsPerSecond * t.window.Seconds()); err != nil {
		return errors.Wrap(err, "ReadsPerSecond")
	}

	if err := t.writeOps.SetLimit(limits.WritesPerSecond * t.window.Seconds()); err != nil {
		return errors.Wrap(err, "WritesPerSecond")
	}

	if err := t.listOps.SetLimit(limits.ListsPerSecond * t.window.Seconds()); err != nil {
		return errors.Wrap(err, "ListsPerSecond")
	}

	if err := t.upload.SetLimit(limits.UploadBytesPerSecond * t.window.Seconds()); err != nil {
		return errors.Wrap(err, "UploadBytesPerSecond")
	}

	if err := t.download.SetLimit(limits.DownloadBytesPerSecond * t.window.Seconds()); err != nil {
		return errors.Wrap(err, "DownloadBytesPerSecond")
	}

	if err := t.concurrentReads.SetLimit(limits.ConcurrentReads); err != nil {
		return errors.Wrap(err, "ConcurrentReads")
	}

	if err := t.concurrentWrites.SetLimit(limits.ConcurrentWrites); err != nil {
		return errors.Wrap(err, "ConcurrentWrites")
	}

	return nil
}

func (t *tokenBucketBasedThrottler) OnUpdate(handler UpdatedHandler) {
	t.onUpdate = append(t.onUpdate, handler)
}

// Limits encapsulates all limits for a Throttler.
type Limits struct {
	ReadsPerSecond         float64 `json:"readsPerSecond,omitempty"`
	WritesPerSecond        float64 `json:"writesPerSecond,omitempty"`
	ListsPerSecond         float64 `json:"listsPerSecond,omitempty"`
	UploadBytesPerSecond   float64 `json:"maxUploadSpeedBytesPerSecond,omitempty"`
	DownloadBytesPerSecond float64 `json:"maxDownloadSpeedBytesPerSecond,omitempty"`
	ConcurrentReads        int     `json:"concurrentReads,omitempty"`
	ConcurrentWrites       int     `json:"concurrentWrites,omitempty"`
}

var _ Throttler = (*tokenBucketBasedThrottler)(nil)

// NewThrottler returns a Throttler with provided limits.
func NewThrottler(limits Limits, window time.Duration, initialFillRatio float64) (SettableThrottler, error) {
	t := &tokenBucketBasedThrottler{
		readOps:          newTokenBucket("read-ops", initialFillRatio*limits.ReadsPerSecond*window.Seconds(), 0, window),
		writeOps:         newTokenBucket("write-ops", initialFillRatio*limits.WritesPerSecond*window.Seconds(), 0, window),
		listOps:          newTokenBucket("list-ops", initialFillRatio*limits.ListsPerSecond*window.Seconds(), 0, window),
		upload:           newTokenBucket("upload-bytes", initialFillRatio*limits.UploadBytesPerSecond*window.Seconds(), 0, window),
		download:         newTokenBucket("download-bytes", initialFillRatio*limits.DownloadBytesPerSecond*window.Seconds(), 0, window),
		concurrentReads:  newSemaphore(),
		concurrentWrites: newSemaphore(),
		window:           window,
	}

	if err := t.SetLimits(limits); err != nil {
		return nil, errors.Wrap(err, "invalid limits")
	}

	return t, nil
}
