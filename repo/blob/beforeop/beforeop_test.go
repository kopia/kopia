package beforeop

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestBeforeOpStorageNegative(t *testing.T) {
	r := NewWrapper(blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, clock.Now),
		func(ctx context.Context, id blob.ID) error {
			return errors.Wrap(blob.ErrBlobNotFound, "GetBlob error")
		},
		func(ctx context.Context) error {
			return errors.Wrap(blob.ErrBlobNotFound, "GetMetadata error")
		},
		func(ctx context.Context) error {
			return errors.Wrap(blob.ErrBlobNotFound, "DeleteBlob error")
		},
		func(ctx context.Context, id blob.ID, opts *blob.PutOptions) error {
			return errors.Wrap(blob.ErrBlobNotFound, "PutBlob error")
		},
	)

	var data gather.WriteBuffer
	defer data.Close()

	err := r.GetBlob(testlogging.Context(t), "id", 0, 0, &data)
	require.Errorf(t, err, "GetBlob error")

	err = r.PutBlob(testlogging.Context(t), "id", data.Bytes(), blob.PutOptions{})
	require.Errorf(t, err, "PutBlob error")

	err = r.DeleteBlob(testlogging.Context(t), "id")
	require.Errorf(t, err, "DeleteBlob error")

	_, err = r.GetMetadata(testlogging.Context(t), "id")
	require.Errorf(t, err, "GetMetadata error")
}

func TestBeforeOpStoragePositive(t *testing.T) {
	var getBlobCbInvoked, getBlobMetadataCbInvoked, putBlobCbInvoked, deleteBlobCbInvoked bool

	r := NewWrapper(blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, clock.Now),
		func(ctx context.Context, id blob.ID) error {
			getBlobCbInvoked = true
			return nil
		},
		func(ctx context.Context) error {
			getBlobMetadataCbInvoked = true
			return nil
		},
		func(ctx context.Context) error {
			deleteBlobCbInvoked = true
			return nil
		},
		func(ctx context.Context, id blob.ID, opts *blob.PutOptions) error {
			putBlobCbInvoked = true
			return nil
		},
	)

	var data gather.WriteBuffer
	defer data.Close()

	_ = r.GetBlob(testlogging.Context(t), "id", 0, 0, &data)
	require.True(t, getBlobCbInvoked)

	_ = r.PutBlob(testlogging.Context(t), "id", data.Bytes(), blob.PutOptions{})
	require.True(t, putBlobCbInvoked)

	_ = r.DeleteBlob(testlogging.Context(t), "id")
	require.True(t, deleteBlobCbInvoked)

	_, _ = r.GetMetadata(testlogging.Context(t), "id")
	require.True(t, getBlobMetadataCbInvoked)
}
