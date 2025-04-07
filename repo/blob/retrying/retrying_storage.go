// Package retrying implements wrapper around blob.Storage that adds retry loop around all operations in case they return unexpected errors.
package retrying

import (
	"context"
	"errors"
	"fmt"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

// retryingStorage adds retry loop around all operations of the underlying storage.
type retryingStorage struct {
	blob.Storage
}

func (s retryingStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	return retry.WithExponentialBackoffNoValue(ctx, fmt.Sprintf("GetBlob(%v,%v,%v)", id, offset, length), func() error {
		output.Reset()

		return s.Storage.GetBlob(ctx, id, offset, length, output)
	}, isRetriable)
}

func (s retryingStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	return retry.WithExponentialBackoff(ctx, "GetMetadata("+string(id)+")", func() (blob.Metadata, error) {
		return s.Storage.GetMetadata(ctx, id)
	}, isRetriable)
}

func (s retryingStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	return retry.WithExponentialBackoffNoValue(ctx, "PutBlob("+string(id)+")", func() error {
		return s.Storage.PutBlob(ctx, id, data, opts)
	}, isRetriable)
}

func (s retryingStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	return retry.WithExponentialBackoffNoValue(ctx, "DeleteBlob("+string(id)+")", func() error {
		return s.Storage.DeleteBlob(ctx, id)
	}, isRetriable)
}

// NewWrapper returns a Storage wrapper that adds retry loop around all operations of the underlying storage.
func NewWrapper(wrapped blob.Storage) blob.Storage {
	return &retryingStorage{Storage: wrapped}
}

func isRetriable(err error) bool {
	switch {
	case errors.Is(err, blob.ErrBlobNotFound):
		return false

	case errors.Is(err, blob.ErrInvalidRange):
		return false

	case errors.Is(err, blob.ErrSetTimeUnsupported):
		return false

	case errors.Is(err, blob.ErrInvalidCredentials):
		return false

	case errors.Is(err, blob.ErrUnsupportedPutBlobOption):
		return false

	case errors.Is(err, blob.ErrBlobAlreadyExists):
		return false

	case errors.Is(err, repo.ErrRepositoryUnavailableDueToUpgradeInProgress):
		// hard-fail when upgrade is in progress
		return false

	default:
		return true
	}
}
