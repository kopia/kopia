// Package beforeop implements wrapper around blob.Storage that run a given callback before all operations.
package beforeop

import (
	"context"

	"github.com/kopia/kopia/repo/blob"
)

type (
	callback          func(ctx context.Context) error
	onGetBlobCallback func(ctx context.Context, id blob.ID) error
	onPutBlobCallback func(ctx context.Context, id blob.ID, opts *blob.PutOptions) error // allows mutating the put-options
)

type beforeOp struct {
	blob.Storage
	onGetMetadata, onDeleteBlob callback
	onGetBlob                   onGetBlobCallback
	onPutBlob                   onPutBlobCallback
}

func (s beforeOp) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	if s.onGetBlob != nil {
		if err := s.onGetBlob(ctx, id); err != nil {
			return err
		}
	}

	return s.Storage.GetBlob(ctx, id, offset, length, output) //nolint:wrapcheck
}

func (s beforeOp) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	if s.onGetMetadata != nil {
		if err := s.onGetMetadata(ctx); err != nil {
			return blob.Metadata{}, err
		}
	}

	return s.Storage.GetMetadata(ctx, id) //nolint:wrapcheck
}

func (s beforeOp) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if s.onPutBlob != nil {
		if err := s.onPutBlob(ctx, id, &opts); err != nil {
			return err
		}
	}

	return s.Storage.PutBlob(ctx, id, data, opts) //nolint:wrapcheck
}

func (s beforeOp) DeleteBlob(ctx context.Context, id blob.ID) error {
	if s.onDeleteBlob != nil {
		if err := s.onDeleteBlob(ctx); err != nil {
			return err
		}
	}

	return s.Storage.DeleteBlob(ctx, id) //nolint:wrapcheck
}

// NewWrapper creates a wrapped storage interface for data operations that need
// to run a callback before the actual operation.
func NewWrapper(wrapped blob.Storage, onGetBlob onGetBlobCallback, onGetMetadata, onDeleteBlob callback, onPutBlob onPutBlobCallback) blob.Storage {
	return &beforeOp{
		Storage:       wrapped,
		onGetBlob:     onGetBlob,
		onGetMetadata: onGetMetadata,
		onDeleteBlob:  onDeleteBlob,
		onPutBlob:     onPutBlob,
	}
}

// NewUniformWrapper is same as NewWrapper except that it only accepts a single
// uniform callback for all the operations for simpler use-cases.
func NewUniformWrapper(wrapped blob.Storage, cb callback) blob.Storage {
	return &beforeOp{
		Storage:       wrapped,
		onGetBlob:     func(ctx context.Context, _ blob.ID) error { return cb(ctx) },
		onGetMetadata: cb,
		onDeleteBlob:  cb,
		onPutBlob:     func(ctx context.Context, _ blob.ID, _ *blob.PutOptions) error { return cb(ctx) },
	}
}
