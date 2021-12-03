package beforeop

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestBeforeOpStorageNegative(t *testing.T) {
	r := NewWrapper(blobtesting.NewMapStorage(nil, nil, clock.Now),
		func(id blob.ID) error {
			return errors.Wrap(blob.ErrBlobNotFound, "GetBlob error")
		},
		func() error {
			return errors.Wrap(blob.ErrBlobNotFound, "GetMetadata error")
		},
		func() error {
			return errors.Wrap(blob.ErrBlobNotFound, "DeleteBlob error")
		},
		func(id blob.ID, opts *blob.PutOptions) error {
			return errors.Wrap(blob.ErrBlobNotFound, "PutBlob error")
		},
	)

	err := r.GetBlob(testlogging.Context(t), "id", 0, 0, nil)
	require.Errorf(t, err, "GetBlob error")

	err = r.PutBlob(testlogging.Context(t), "id", nil, blob.PutOptions{})
	require.Errorf(t, err, "PutBlob error")

	err = r.DeleteBlob(testlogging.Context(t), "id")
	require.Errorf(t, err, "DeleteBlob error")

	_, err = r.GetMetadata(testlogging.Context(t), "id")
	require.Errorf(t, err, "GetMetadata error")
}
