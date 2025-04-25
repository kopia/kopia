// Package timeout wraps blob.Storage to use ctx.WithTimeout on all requests.
package timeout

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo/blob"
)

// withRequestTimeout adds a deadline to the context if RequestTimeout is greater than 0.
// Returns the updated context and a cancel function that should be called to free resources.
func withRequestTimeout(ctx context.Context, seconds int64) (context.Context, context.CancelFunc) {
	if seconds > 0 {
		return context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	}

	// If no timeout, return the original context and a no-op cancel function.
	return ctx, func() {}
}

// StorageTimeoutWrapper is the struct that implements the StorateTimeout
// interface, it will store the blob.Storage variable and wrap around.
type StorageTimeoutWrapper struct {
	blob.Storage

	RequestTimeoutSeconds int64
}

// PutBlob is a wrapper around blob.Storage.PutBlob.
func (s *StorageTimeoutWrapper) PutBlob(ctx context.Context, blobID blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	ctx, cancel := withRequestTimeout(ctx, s.RequestTimeoutSeconds)
	defer cancel()

	return s.Storage.PutBlob(ctx, blobID, data, opts) //nolint:wrapcheck
}

// DeleteBlob is a wrapper around blob.Storage.DeleteBlob.
func (s *StorageTimeoutWrapper) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	ctx, cancel := withRequestTimeout(ctx, s.RequestTimeoutSeconds)
	defer cancel()

	return s.Storage.DeleteBlob(ctx, blobID) //nolint:wrapcheck
}

// ExtendBlobRetention is a wrapper around blob.Storage.ExtendBlobRetention.
func (s *StorageTimeoutWrapper) ExtendBlobRetention(ctx context.Context, blobID blob.ID, opts blob.ExtendOptions) error {
	ctx, cancel := withRequestTimeout(ctx, s.RequestTimeoutSeconds)
	defer cancel()

	return s.Storage.ExtendBlobRetention(ctx, blobID, opts) //nolint:wrapcheck
}

// GetBlob is a wrapper around blob.Storage.GetBlob.
func (s *StorageTimeoutWrapper) GetBlob(ctx context.Context, blobID blob.ID, offset, length int64, output blob.OutputBuffer) error {
	ctx, cancel := withRequestTimeout(ctx, s.RequestTimeoutSeconds)
	defer cancel()

	return s.Storage.GetBlob(ctx, blobID, offset, length, output) //nolint:wrapcheck
}

// GetMetadata is a wrapper around blob.Storage.GetMetadata.
func (s *StorageTimeoutWrapper) GetMetadata(ctx context.Context, blobID blob.ID) (blob.Metadata, error) {
	ctx, cancel := withRequestTimeout(ctx, s.RequestTimeoutSeconds)
	defer cancel()

	return s.Storage.GetMetadata(ctx, blobID) //nolint:wrapcheck
}

// ListBlobs is a wrapper around blob.Storage.ListBlobs.
func (s *StorageTimeoutWrapper) ListBlobs(ctx context.Context, blobIDPrefix blob.ID, cb func(bm blob.Metadata) error) error {
	ctx, cancel := withRequestTimeout(ctx, s.RequestTimeoutSeconds)
	defer cancel()

	return s.Storage.ListBlobs(ctx, blobIDPrefix, cb) //nolint:wrapcheck
}

// NewStorageTimeout returns a StorageTimeout a wrapper around Storage.
func NewStorageTimeout(s blob.Storage, timeoutSeconds int64) blob.Storage {
	return &StorageTimeoutWrapper{
		Storage:               s,
		RequestTimeoutSeconds: timeoutSeconds,
	}
}
