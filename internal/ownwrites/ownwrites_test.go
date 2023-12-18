package ownwrites

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

const testCacheDuration = 15 * time.Minute

func TestOwnWrites(t *testing.T) {
	realStorageTime := faketime.NewTimeAdvance(time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC))
	realStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, realStorageTime.NowFunc())
	cacheTime := faketime.NewTimeAdvance(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC))
	cachest := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, cacheTime.NowFunc())

	ec := blobtesting.NewEventuallyConsistentStorage(realStorage, 1*time.Hour, realStorageTime.NowFunc())
	ow := NewWrapper(ec, cachest, []blob.ID{"n"}, testCacheDuration)
	ow.(*CacheStorage).cacheTimeFunc = cacheTime.NowFunc()

	ctx := testlogging.Context(t)

	// seed some blobs into storage and advance time so they are reliably settled.
	require.NoError(t, ec.PutBlob(ctx, "npreexisting", gather.FromSlice([]byte("pre-existing")), blob.PutOptions{}))
	realStorageTime.Advance(1 * time.Hour)

	require.NoError(t, ow.PutBlob(ctx, "n123", gather.FromSlice([]byte("not-important")), blob.PutOptions{}))
	// verify we wrote the marker into cache.
	blobtesting.AssertGetBlob(ctx, t, cachest, "addn123", []byte("marker"))

	require.NoError(t, ow.PutBlob(ctx, "x123", gather.FromSlice([]byte("not-important")), blob.PutOptions{}))
	blobtesting.AssertGetBlobNotFound(ctx, t, cachest, "addx123")

	// make sure eventual consistency wrapper won't return the item yet.
	blobtesting.AssertListResultsIDs(ctx, t, ec, "n", "npreexisting")

	// despite that our wrapper will have it
	blobtesting.AssertListResultsIDs(ctx, t, ow, "n", "n123", "npreexisting")

	// move time now, so that eventual consistency is settled.
	realStorageTime.Advance(1 * time.Hour)

	// both storages will now agree
	blobtesting.AssertListResultsIDs(ctx, t, ec, "n", "n123", "npreexisting")
	blobtesting.AssertListResultsIDs(ctx, t, ow, "n", "n123", "npreexisting")

	cacheTime.Advance(1 * time.Minute)

	require.NoError(t, ow.DeleteBlob(ctx, "n123"))

	// verify we wrote the marker into cache.
	blobtesting.AssertGetBlob(ctx, t, cachest, "deln123", []byte("marker"))

	// ec still has the deleted blob
	blobtesting.AssertListResultsIDs(ctx, t, ec, "n", "n123", "npreexisting")

	// but we hide it from results.
	blobtesting.AssertListResultsIDs(ctx, t, ow, "n", "npreexisting")

	blobtesting.AssertListResultsIDs(ctx, t, cachest, "", "addn123", "deln123")

	cacheTime.Advance(24 * time.Hour)
	realStorageTime.Advance(24 * time.Hour)

	blobtesting.AssertListResultsIDs(ctx, t, ow, "n", "npreexisting")

	// make sure cache got sweeped
	blobtesting.AssertListResultsIDs(ctx, t, cachest, "")
}
