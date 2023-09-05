// Package blobtesting implements storage with fault injection.
package blobtesting

import (
	"context"

	"github.com/kopia/kopia/internal/fault"
	"github.com/kopia/kopia/repo/blob"
)

// Supported faulty methods.
const (
	MethodGetBlob fault.Method = iota
	MethodGetMetadata
	MethodPutBlob
	MethodDeleteBlob
	MethodListBlobs
	MethodListBlobsItem
	MethodClose
	MethodFlushCaches
	MethodGetCapacity
)

// FaultyStorage implements fault injection for FaultyStorage.
type FaultyStorage struct {
	base blob.Storage

	*fault.Set
}

// NewFaultyStorage creates new Storage with fault injection.
func NewFaultyStorage(base blob.Storage) *FaultyStorage {
	return &FaultyStorage{
		base: base,
		Set:  fault.NewSet(),
	}
}

func (s *FaultyStorage) IsReadOnly() bool {
	return s.base.IsReadOnly()
}

// GetCapacity implements blob.Volume.
func (s *FaultyStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	if ok, err := s.GetNextFault(ctx, MethodGetCapacity); ok {
		return blob.Capacity{}, err
	}

	return s.base.GetCapacity(ctx)
}

// GetBlob implements blob.Storage.
func (s *FaultyStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if ok, err := s.GetNextFault(ctx, MethodGetBlob, id, offset, length); ok {
		return err
	}

	return s.base.GetBlob(ctx, id, offset, length, output)
}

// GetMetadata implements blob.Storage.
func (s *FaultyStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	if ok, err := s.GetNextFault(ctx, MethodGetMetadata, id); ok {
		return blob.Metadata{}, err
	}

	return s.base.GetMetadata(ctx, id)
}

// PutBlob implements blob.Storage.
func (s *FaultyStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if ok, err := s.GetNextFault(ctx, MethodPutBlob, id); ok {
		return err
	}

	return s.base.PutBlob(ctx, id, data, opts)
}

// DeleteBlob implements blob.Storage.
func (s *FaultyStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	if ok, err := s.GetNextFault(ctx, MethodDeleteBlob, id); ok {
		return err
	}

	return s.base.DeleteBlob(ctx, id)
}

// ListBlobs implements blob.Storage.
func (s *FaultyStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	if ok, err := s.GetNextFault(ctx, MethodListBlobs, prefix); ok {
		return err
	}

	return s.base.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		if ok, err := s.GetNextFault(ctx, MethodListBlobsItem, prefix); ok {
			return err
		}
		return callback(bm)
	})
}

// Close implements blob.Storage.
func (s *FaultyStorage) Close(ctx context.Context) error {
	if ok, err := s.GetNextFault(ctx, MethodClose); ok {
		return err
	}

	return s.base.Close(ctx)
}

// ConnectionInfo implements blob.Storage.
func (s *FaultyStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.base.ConnectionInfo()
}

// DisplayName implements blob.Storage.
func (s *FaultyStorage) DisplayName() string {
	return s.base.DisplayName()
}

// FlushCaches implements blob.Storage.
func (s *FaultyStorage) FlushCaches(ctx context.Context) error {
	if ok, err := s.GetNextFault(ctx, MethodFlushCaches); ok {
		return err
	}

	return s.base.FlushCaches(ctx)
}

// ExtendBlobRetention implements blob.Storage.
func (s *FaultyStorage) ExtendBlobRetention(ctx context.Context, b blob.ID, opts blob.ExtendOptions) error {
	return s.base.ExtendBlobRetention(ctx, b, opts)
}

var _ blob.Storage = (*FaultyStorage)(nil)
