package listcache

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

var errFake = errors.New("fake")

func TestListCache(t *testing.T) {
	realStorageTime := faketime.NewTimeAdvance(time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC))
	realStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, realStorageTime.NowFunc())
	cacheTime := faketime.NewTimeAdvance(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC))
	cachest := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, cacheTime.NowFunc())

	lc := NewWrapper(realStorage, cachest, []blob.ID{"n", "xe", "xb"}, []byte("hmac-secret"), 1*time.Minute).(*listCacheStorage)
	lc.cacheTimeFunc = cacheTime.NowFunc()

	ctx := testlogging.Context(t)

	blobtesting.AssertListResultsIDs(ctx, t, cachest, "")
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n")
	// cached blob gets written
	blobtesting.AssertListResultsIDs(ctx, t, cachest, "", "n")
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n")

	// modify underlying storage without going through cache layer
	require.NoError(t, realStorage.PutBlob(ctx, "n1", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	// still getting empty cached results.
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n")

	// cache expires, real data is read
	cacheTime.Advance(1 * time.Hour)
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1")
	require.NoError(t, realStorage.PutBlob(ctx, "n2", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	// n2 still invisible, "n" is cached.
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1")

	// writing "n3" through the cache storage invalidates "n".
	require.NoError(t, lc.PutBlob(ctx, "n3", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n2", "n3")

	// deleting "n2" through the cache storage invalidates "n".
	require.NoError(t, lc.DeleteBlob(ctx, "n2"))
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3")

	// add one more blob.
	require.NoError(t, realStorage.PutBlob(ctx, "n4", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	// replace "n" in cache storage with invalid data.
	require.NoError(t, cachest.PutBlob(ctx, "n", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	// on next read, "n" will be discarded and "n4" will be immediately visible.
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3", "n4")

	cacheTime.Advance(1 * time.Hour)

	// add one more blob.
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3", "n4")
	require.NoError(t, realStorage.PutBlob(ctx, "n5", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3", "n4")
	cacheTime.Advance(lc.cacheDuration - 1)
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3", "n4")
	cacheTime.Advance(1)
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3", "n4", "n5")

	// explicit flush
	require.NoError(t, realStorage.PutBlob(ctx, "n6", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3", "n4", "n5")
	require.NoError(t, lc.FlushCaches(ctx))
	blobtesting.AssertListResultsIDs(ctx, t, lc, "n", "n1", "n3", "n4", "n5", "n6")

	// non-cached results
	blobtesting.AssertListResultsIDs(ctx, t, lc, "nc")
	require.NoError(t, realStorage.PutBlob(ctx, "nc1", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	blobtesting.AssertListResultsIDs(ctx, t, lc, "nc", "nc1")

	require.ErrorIs(t, lc.ListBlobs(ctx, "n", func(m blob.Metadata) error {
		return errFake
	}), errFake)
}
