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
	// nolint:wrapcheck
	return retry.WithExponentialBackoffNoValue(ctx, fmt.Sprintf("GetBlob(%v,%v,%v)", id, offset, length), func() error {
		output.Reset()

		// nolint:wrapcheck
		return s.Storage.GetBlob(ctx, id, offset, length, output)
	}, isRetriable)
}

func (s retryingStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	v, err := retry.WithExponentialBackoff(ctx, "GetMetadata("+string(id)+")", func() (interface{}, error) {
		// nolint:wrapcheck
		return s.Storage.GetMetadata(ctx, id)
	}, isRetriable)
	if err != nil {
		return blob.Metadata{}, err // nolint:wrapcheck
	}

	return v.(blob.Metadata), nil // nolint:forcetypeassert
}

func (s retryingStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	_, err := retry.WithExponentialBackoff(ctx, "PutBlob("+string(id)+")", func() (interface{}, error) {
		// nolint:wrapcheck
		return true, s.Storage.PutBlob(ctx, id, data, opts)
	}, isRetriable)

	return err // nolint:wrapcheck
}

func (s retryingStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	_, err := retry.WithExponentialBackoff(ctx, "DeleteBlob("+string(id)+")", func() (interface{}, error) {
		// nolint:wrapcheck
		return true, s.Storage.DeleteBlob(ctx, id)
	}, isRetriable)

	return err // nolint:wrapcheck
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

	case errors.Is(err, repo.ErrRepositoryUnavailableDueToUpgrageInProgress):
		// hard-fail when upgrade is in progress
		return false

	default:
		return true
	}
}
