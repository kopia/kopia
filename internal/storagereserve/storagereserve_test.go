package storagereserve

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

func TestStorageReserve(t *testing.T) {
	ctx := context.Background()
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Check initially missing
	exists, err := Exists(ctx, st)
	require.NoError(t, err)
	require.False(t, exists)

	// Create reserve
	size := int64(1024)
	err = Create(ctx, st, size)
	require.NoError(t, err)

	// Verify exists
	exists, err = Exists(ctx, st)
	require.NoError(t, err)
	require.True(t, exists)

	// Verify metadata
	bm, err := st.GetMetadata(ctx, blob.ID(ReserveBlobID))
	require.NoError(t, err)
	require.Equal(t, size, bm.Length)

	// Verify content is zeros
	var buf gather.WriteBuffer
	defer buf.Close()
	err = st.GetBlob(ctx, ReserveBlobID, 0, -1, &buf)
	require.NoError(t, err)
	for _, b := range buf.ToByteSlice() {
		require.Equal(t, byte(0), b)
	}

	// Delete reserve
	err = Delete(ctx, st)
	require.NoError(t, err)

	// Verify missing again
	exists, err = Exists(ctx, st)
	require.NoError(t, err)
	require.False(t, exists)

	// Ensure creates it
	err = Ensure(ctx, st, size)
	require.NoError(t, err)
	exists, err = Exists(ctx, st)
	require.NoError(t, err)
	require.True(t, exists)

	// Ensure when already exists does nothing
	err = Ensure(ctx, st, size)
	require.NoError(t, err)
}