package blobtesting

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

func TestMapStorage(t *testing.T) {
	data := DataMap{}

	r := NewMapStorage(data, nil, nil)
	if r == nil {
		t.Errorf("unexpected result: %v", r)
	}

	VerifyStorage(testlogging.Context(t), t, r, blob.PutOptions{})
}

func TestMapStorageWithLimit(t *testing.T) {
	ctx := testlogging.Context(t)
	data := DataMap{}

	r := NewMapStorageWithLimit(data, nil, nil, 10)
	verifyCapacityAndFreeSpace(t, r, 10, 10)
	require.NoError(t, r.PutBlob(ctx, "foo", gather.FromSlice([]byte("foo")), blob.PutOptions{}))
	verifyCapacityAndFreeSpace(t, r, 10, 7)
	require.NoError(t, r.PutBlob(ctx, "bar", gather.FromSlice([]byte("bar")), blob.PutOptions{}))
	verifyCapacityAndFreeSpace(t, r, 10, 4)
	require.NoError(t, r.PutBlob(ctx, "baz", gather.FromSlice([]byte("baz")), blob.PutOptions{}))
	verifyCapacityAndFreeSpace(t, r, 10, 1)

	// we're at 9/10 bytes, can't add 3 more
	require.ErrorContains(t, r.PutBlob(ctx, "qux", gather.FromSlice([]byte("qux")), blob.PutOptions{}), "exceeded limit")
	// remove 3 bytes
	require.NoError(t, r.DeleteBlob(ctx, "baz"))
	verifyCapacityAndFreeSpace(t, r, 10, 4)
	// can add 4 bytes again
	require.NoError(t, r.PutBlob(ctx, "qux", gather.FromSlice([]byte("qux1")), blob.PutOptions{}))
	verifyCapacityAndFreeSpace(t, r, 10, 0)
	// can't add any more bytes since we're at 10/10 bytes
	require.ErrorContains(t, r.PutBlob(ctx, "aaa", gather.FromSlice([]byte("1")), blob.PutOptions{}), "exceeded limit")
	// adding zero bytes won't fail in this situation.
	require.NoError(t, r.PutBlob(ctx, "bbb", gather.FromSlice([]byte{}), blob.PutOptions{}), "exceeded limit")
	verifyCapacityAndFreeSpace(t, r, 10, 0)

	r = NewMapStorageWithLimit(DataMap{
		"foo": []byte("foo"),
	}, nil, nil, 20)
	verifyCapacityAndFreeSpace(t, r, 20, 17)
}

func verifyCapacityAndFreeSpace(t *testing.T, r blob.Storage, wantSize, wantFree int64) {
	t.Helper()

	c, err := r.GetCapacity(testlogging.Context(t))
	require.NoError(t, err)

	require.Equal(t, uint64(wantSize), c.SizeB)
	require.Equal(t, uint64(wantFree), c.FreeB)
}
