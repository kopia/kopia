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
}

type tokenBucketBasedThrottler struct {
	mu sync.Mutex
	// +checklocks:mu
	limits Limits

	readOps  *tokenBucket
	writeOps *tokenBucket
	listOps  *tokenBucket
	upload   *tokenBucket
	download *tokenBucket
	window   time.Duration // +checklocksignore
}

func (t *tokenBucketBasedThrottler) BeforeOperation(ctx context.Context, op string) {
	switch op {
	case operationListBlobs:
		t.listOps.Take(ctx, 1)
	case operationGetBlob, operationGetMetadata:
		t.readOps.Take(ctx, 1)
	case operationPutBlob, operationDeleteBlob:
		t.writeOps.Take(ctx, 1)
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

	t.limits = limits

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

	return nil
}

// Limits encapsulates all limits for a Throttler.
type Limits struct {
	ReadsPerSecond         float64 `json:"readsPerSecond,omitempty"`
	WritesPerSecond        float64 `json:"writesPerSecond,omitempty"`
	ListsPerSecond         float64 `json:"listsPerSecond,omitempty"`
	UploadBytesPerSecond   float64 `json:"maxUploadSpeedBytesPerSecond,omitempty"`
	DownloadBytesPerSecond float64 `json:"maxDownloadSpeedBytesPerSecond,omitempty"`
}

var _ Throttler = (*tokenBucketBasedThrottler)(nil)

// NewThrottler returns a Throttler with provided limits.
func NewThrottler(limits Limits, window time.Duration, initialFillRatio float64) (SettableThrottler, error) {
	t := &tokenBucketBasedThrottler{
		readOps:  newTokenBucket("read-ops", initialFillRatio*limits.ReadsPerSecond*window.Seconds(), 0, window),
		writeOps: newTokenBucket("write-ops", initialFillRatio*limits.WritesPerSecond*window.Seconds(), 0, window),
		listOps:  newTokenBucket("list-ops", initialFillRatio*limits.ListsPerSecond*window.Seconds(), 0, window),
		upload:   newTokenBucket("upload-bytes", initialFillRatio*limits.UploadBytesPerSecond*window.Seconds(), 0, window),
		download: newTokenBucket("download-bytes", initialFillRatio*limits.DownloadBytesPerSecond*window.Seconds(), 0, window),
		window:   window,
	}

	if err := t.SetLimits(limits); err != nil {
		return nil, errors.Wrap(err, "invalid limits")
	}

	return t, nil
}
