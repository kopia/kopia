// Package readonly implements wrapper around readonlyStorage that prevents all mutations.
package readonly

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

// ErrReadonly returns an error indicating that storage is read only.
var ErrReadonly = errors.New("storage is read-only")

// readonlyStorage prevents all mutations on the underlying storage.
type readonlyStorage struct {
	base blob.Storage
	blob.DefaultProviderImplementation
}

func (s readonlyStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	//nolint:wrapcheck
	return s.base.GetCapacity(ctx)
}

func (s readonlyStorage) IsReadOnly() bool {
	return true
}

func (s readonlyStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	//nolint:wrapcheck
	return s.base.GetBlob(ctx, id, offset, length, output)
}

func (s readonlyStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	//nolint:wrapcheck
	return s.base.GetMetadata(ctx, id)
}

//nolint:revive
func (s readonlyStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	return ErrReadonly
}

//nolint:revive
func (s readonlyStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	return ErrReadonly
}

func (s readonlyStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	//nolint:wrapcheck
	return s.base.ListBlobs(ctx, prefix, callback)
}

func (s readonlyStorage) Close(ctx context.Context) error {
	//nolint:wrapcheck
	return s.base.Close(ctx)
}

func (s readonlyStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.base.ConnectionInfo()
}

func (s readonlyStorage) DisplayName() string {
	return s.base.DisplayName()
}

func (s readonlyStorage) FlushCaches(ctx context.Context) error {
	//nolint:wrapcheck
	return s.base.FlushCaches(ctx)
}

// NewWrapper returns a readonly Storage wrapper that prevents any mutations to the underlying storage.
func NewWrapper(wrapped blob.Storage) blob.Storage {
	return &readonlyStorage{base: wrapped}
}
