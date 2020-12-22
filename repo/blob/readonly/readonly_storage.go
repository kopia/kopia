// Package readonly implements wrapper around readonlyStorage that prevents all mutations.
package readonly

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// ErrReadonly returns an error indicating that storage is read only.
var ErrReadonly = errors.Errorf("storage is read-only")

// readonlyStorage prevents all mutations on the underlying storage.
type readonlyStorage struct {
	base blob.Storage
}

func (s readonlyStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64) ([]byte, error) {
	return s.base.GetBlob(ctx, id, offset, length)
}

func (s readonlyStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	return s.base.GetMetadata(ctx, id)
}

func (s readonlyStorage) SetTime(ctx context.Context, id blob.ID, t time.Time) error {
	// nolint:wrapcheck
	return ErrReadonly
}

func (s readonlyStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes) error {
	// nolint:wrapcheck
	return ErrReadonly
}

func (s readonlyStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	// nolint:wrapcheck
	return ErrReadonly
}

func (s readonlyStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	return s.base.ListBlobs(ctx, prefix, callback)
}

func (s readonlyStorage) Close(ctx context.Context) error {
	return s.base.Close(ctx)
}

func (s readonlyStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.base.ConnectionInfo()
}

func (s readonlyStorage) DisplayName() string {
	return s.base.DisplayName()
}

// NewWrapper returns a readonly Storage wrapper that prevents any mutations to the underlying storage.
func NewWrapper(wrapped blob.Storage) blob.Storage {
	return &readonlyStorage{base: wrapped}
}
