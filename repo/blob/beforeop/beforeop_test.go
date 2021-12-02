package beforeop

import (
	"fmt"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/stretchr/testify/require"
)

func TestBeforeOpStorageNegative(t *testing.T) {
	r := NewWrapper(blobtesting.NewMapStorage(nil, nil, clock.Now),
		func() error {
			return fmt.Errorf("GetBlob error")
		},
		func() error {
			return fmt.Errorf("GetMetadata error")
		},
		func() error {
			return fmt.Errorf("DeleteBlob error")
		},
		func(id blob.ID, opts *blob.PutOptions) error {
			return fmt.Errorf("PutBlob error")
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
