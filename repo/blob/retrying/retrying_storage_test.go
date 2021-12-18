package retrying_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

func TestRetrying(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	someError := errors.New("some error")
	ms := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)
	fs := blobtesting.NewFaultyStorage(ms)
	fs.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)
	fs.AddFault(blobtesting.MethodGetBlob).ErrorInstead(someError)
	fs.AddFault(blobtesting.MethodGetMetadata).ErrorInstead(someError)
	fs.AddFault(blobtesting.MethodDeleteBlob).ErrorInstead(someError)

	rs := retrying.NewWrapper(fs)
	blobID := blob.ID("deadcafe")
	blobID2 := blob.ID("deadcafe2")

	require.NoError(t, rs.PutBlob(ctx, blobID, gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	require.NoError(t, rs.PutBlob(ctx, blobID2, gather.FromSlice([]byte{1, 2, 3, 4}), blob.PutOptions{}))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err := rs.GetBlob(ctx, blobID, 0, -1, &tmp)
	require.NoError(t, err)

	_, err = rs.GetMetadata(ctx, blobID)
	require.NoError(t, err)

	if err = rs.GetBlob(ctx, blobID, 4, 10000, &tmp); !errors.Is(err, blob.ErrInvalidRange) {
		t.Fatalf("unexpected error: %v", err)
	}

	require.NoError(t, rs.DeleteBlob(ctx, blobID))

	if err = rs.GetBlob(ctx, blobID, 0, -1, &tmp); !errors.Is(err, blob.ErrBlobNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err = rs.GetMetadata(ctx, blobID); !errors.Is(err, blob.ErrBlobNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}

	fs.VerifyAllFaultsExercised(t)
}
