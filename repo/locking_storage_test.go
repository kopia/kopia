package repo_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/format"
)

func TestIsLockingStorageBlobID(t *testing.T) {
	cases := []struct {
		blobID blob.ID
		want   bool
	}{
		{"p0123456789abcdef", true},
		{"q0123456789abcdef", true},
		{"n0123456789abcdef", true},
		{"xn0123456789abcdef", true},
		{"e0123456789abcdef", false},
		{"kopia.repository", true},
		{"kopia.blobcfg", true},
		{"kopia.maintenance", false},
		{"s01234567890abcdef", false},
		{"_log0123456789abcdef", false},
	}

	for _, tc := range cases {
		require.Equal(t, tc.want, repo.IsLockingStorageBlobID(tc.blobID), "blob ID %v", tc.blobID)
	}
}

func TestWrapLockingStorage(t *testing.T) {
	ctx := testlogging.Context(t)

	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ft := faketime.NewTimeAdvance(startTime)
	st := blobtesting.NewVersionedMapStorage(ft.NowFunc())

	wrapped := repo.WrapLockingStorage(st, format.BlobStorageConfiguration{
		RetentionMode:   blob.Governance,
		RetentionPeriod: 24 * time.Hour,
	})

	require.NoError(t, wrapped.PutBlob(ctx, "p0123456789abcdef", gather.FromSlice([]byte("locked")), blob.PutOptions{}))
	require.NoError(t, wrapped.PutBlob(ctx, "kopia.maintenance", gather.FromSlice([]byte("unlocked")), blob.PutOptions{}))

	mode, retainUntil, err := st.GetRetention(ctx, "p0123456789abcdef")
	require.NoError(t, err)
	require.Equal(t, blob.Governance, mode)
	require.Equal(t, startTime.Add(24*time.Hour), retainUntil)

	mode, retainUntil, err = st.GetRetention(ctx, "kopia.maintenance")
	require.NoError(t, err)
	require.Empty(t, mode)
	require.True(t, retainUntil.IsZero())
}
