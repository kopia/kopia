package storagereserve

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

type mockCapacityStorage struct {
	blob.Storage
	cap blob.Capacity
	err error
}

func (s *mockCapacityStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return s.cap, s.err
}

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
	err = st.GetBlob(ctx, blob.ID(ReserveBlobID), 0, -1, &buf)
	require.NoError(t, err)
	for _, b := range buf.ToByteSlice() {
		require.Equal(t, byte(0), b)
	}

	// --- Comprehensive Ensure Path Testing ---

	// Path 1: Emergency Fallback (Full disk, reserve exists)
	// free = 5MB (less than 10MB threshold)
	criticalSt := &mockCapacityStorage{
		Storage: st,
		cap:     blob.Capacity{FreeB: 5 << 20, SizeB: 1 << 30},
	}
	err = Ensure(ctx, criticalSt, size)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInsufficientSpace)
	require.Contains(t, err.Error(), "critical low space")

	// Path 2: Headspace Rule (Not enough space to create)
	// Delete reserve first
	err = Delete(ctx, st)
	require.NoError(t, err)
	
	// Total size = 2GB, Free = 600MB
	// Required = 500MB (reserve) + 200MB (10% headspace) = 700MB.
	// 600MB < 700MB -> should fail.
	headspaceSt := &mockCapacityStorage{
		Storage: st,
		cap:     blob.Capacity{FreeB: 600 << 20, SizeB: 2 << 30},
	}
	err = Ensure(ctx, headspaceSt, 500<<20)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInsufficientSpace)

	// Path 3: Capacity Error Handling (Unexpected error)
	expectedErr := errors.New("disk error")
	errorSt := &mockCapacityStorage{
		Storage: st,
		err:     expectedErr,
	}
	err = Ensure(ctx, errorSt, size)
	require.Error(t, err)
	require.Contains(t, err.Error(), "disk error")

	// Path 4: Success Case (Enough space)
	successSt := &mockCapacityStorage{
		Storage: st,
		cap:     blob.Capacity{FreeB: 1 << 30, SizeB: 2 << 30},
	}
	err = Ensure(ctx, successSt, 500<<20)
	require.NoError(t, err)
	exists, _ = Exists(ctx, st)
	require.True(t, exists)

	// Clean up
	err = Delete(ctx, st)
	require.NoError(t, err)

	// Verify missing again
	exists, err = Exists(ctx, st)
	require.NoError(t, err)
	require.False(t, exists)
}