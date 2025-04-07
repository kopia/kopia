package cache_test

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
)

func newUnderlyingStorageForContentCacheTesting(t *testing.T) blob.Storage {
	t.Helper()

	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	require.NoError(t, st.PutBlob(ctx, "content-1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), blob.PutOptions{}))
	require.NoError(t, st.PutBlob(ctx, "content-4k", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3, 4}, 1000)), blob.PutOptions{})) // 4000 bytes

	return st
}

func TestCacheExpiration_SoftLimitNoMinAge(t *testing.T) {
	// cache is 10k, each blob is 4k, so we can store 2 blobs before they are evicted.
	wantEvicted := []blob.ID{"a", "b"}

	verifyCacheExpiration(t, cache.SweepSettings{
		MaxSizeBytes:   10000,
		TouchThreshold: -1,
	}, wantEvicted)
}

func TestCacheExpiration_SoftLimitWithMinAge(t *testing.T) {
	// cache is 10k, each blob is 4k, cache will grow beyond the limit but will not evict anything.
	verifyCacheExpiration(t, cache.SweepSettings{
		MaxSizeBytes:   10000,
		TouchThreshold: -1,
		MinSweepAge:    time.Hour,
	}, nil)
}

func TestCacheExpiration_HardLimitWithMinAge(t *testing.T) {
	// cache is 10k, each blob is 4k, cache will grow beyond the limit but will not evict anything.
	wantEvicted := []blob.ID{"a", "b"}

	verifyCacheExpiration(t, cache.SweepSettings{
		MaxSizeBytes:   10000,
		TouchThreshold: -1,
		MinSweepAge:    time.Hour,
		LimitBytes:     10000,
	}, wantEvicted)
}

func TestCacheExpiration_HardLimitAboveSoftLimit(t *testing.T) {
	wantExpired := []blob.ID{"a"}

	verifyCacheExpiration(t, cache.SweepSettings{
		MaxSizeBytes:   10000,
		TouchThreshold: -1,
		MinSweepAge:    time.Hour,
		LimitBytes:     13000,
	}, wantExpired)
}

func TestCacheExpiration_HardLimitBelowSoftLimit(t *testing.T) {
	wantExpired := []blob.ID{"a", "b", "c"}

	verifyCacheExpiration(t, cache.SweepSettings{
		MaxSizeBytes:   10000,
		TouchThreshold: -1,
		MinSweepAge:    time.Hour,
		LimitBytes:     5000,
	}, wantExpired)
}

// The test will fetch 4 items into the cache, named "a", "b", "c", "d", each 4000 bytes in size
// verify that the cache is evicting correct items based on the sweep settings.
//
//nolint:thelper
func verifyCacheExpiration(t *testing.T, sweepSettings cache.SweepSettings, wantEvicted []blob.ID) {
	cacheData := blobtesting.DataMap{}

	// on Windows, the time does not always move forward (sometimes clock.Now() returns exactly the same value for consecutive invocations)
	// this matters here so we return a fake clock.Now() function that always moves forward.
	var currentTimeMutex sync.Mutex

	currentTime := clock.Now()

	movingTimeFunc := func() time.Time {
		currentTimeMutex.Lock()
		defer currentTimeMutex.Unlock()

		currentTime = currentTime.Add(1 * time.Millisecond)

		return currentTime
	}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, movingTimeFunc)

	underlyingStorage := newUnderlyingStorageForContentCacheTesting(t)

	ctx := testlogging.Context(t)
	cc, err := cache.NewContentCache(ctx, underlyingStorage, cache.Options{
		Storage: cacheStorage.(cache.Storage),
		Sweep:   sweepSettings,
		TimeNow: movingTimeFunc,
	}, nil)

	require.NoError(t, err)

	defer cc.Close(ctx)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	const underlyingBlobID = "content-4k"

	err = cc.GetContent(ctx, "a", underlyingBlobID, 0, -1, &tmp) // 4k
	require.NoError(t, err)
	err = cc.GetContent(ctx, "b", underlyingBlobID, 0, -1, &tmp) // 4k
	require.NoError(t, err)
	err = cc.GetContent(ctx, "c", underlyingBlobID, 0, -1, &tmp) // 4k
	require.NoError(t, err)
	err = cc.GetContent(ctx, "d", underlyingBlobID, 0, -1, &tmp) // 4k
	require.NoError(t, err)

	// delete underlying storage blob to identify cache items that have been evicted
	// all other items will be fetched from the cache.
	require.NoError(t, underlyingStorage.DeleteBlob(ctx, underlyingBlobID))

	for _, blobID := range []blob.ID{"a", "b", "c", "d"} {
		if slices.Contains(wantEvicted, blobID) {
			require.ErrorIs(t, cc.GetContent(ctx, string(blobID), underlyingBlobID, 0, -1, &tmp), blob.ErrBlobNotFound, "expected item not found %v", blobID)
		} else {
			require.NoError(t, cc.GetContent(ctx, string(blobID), underlyingBlobID, 0, -1, &tmp), "expected item to be found %v", blobID)
		}
	}
}

func TestDiskContentCache(t *testing.T) {
	ctx := testlogging.Context(t)

	tmpDir := testutil.TempDirectory(t)

	const maxBytes = 10000

	cacheStorage, err := cache.NewStorageOrNil(ctx, tmpDir, maxBytes, "contents")
	require.NoError(t, err)

	cc, err := cache.NewContentCache(ctx, newUnderlyingStorageForContentCacheTesting(t), cache.Options{
		Storage: cacheStorage,
		Sweep: cache.SweepSettings{
			MaxSizeBytes: maxBytes,
		},
	}, nil)
	require.NoError(t, err)

	defer cc.Close(ctx)

	verifyContentCache(t, cc, cacheStorage)
}

func verifyContentCache(t *testing.T, cc cache.ContentCache, cacheStorage blob.Storage) {
	t.Helper()

	ctx := testlogging.Context(t)

	t.Run("GetContentContent", func(t *testing.T) {
		cases := []struct {
			contentID string
			blobID    blob.ID
			offset    int64
			length    int64

			expected []byte
			err      error
		}{
			{"xf0f0f1", "content-1", 1, 5, []byte{2, 3, 4, 5, 6}, nil},
			{"xf0f0f2", "content-1", 0, -1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil},
			{"xf0f0f1", "content-1", 1, 5, []byte{2, 3, 4, 5, 6}, nil},
			{"xf0f0f2", "content-1", 0, -1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil},
			{"xf0f0f3", "no-such-content", 0, -1, nil, blob.ErrBlobNotFound},
			{"xf0f0f4", "no-such-content", 10, 5, nil, blob.ErrBlobNotFound},
			{"f0f0f5", "content-1", 7, 3, []byte{8, 9, 10}, nil},
			{"xf0f0f6", "content-1", 11, 10, nil, errors.New("invalid offset: 11: invalid blob offset or length")},
			{"xf0f0f6", "content-1", -1, 5, nil, errors.New("invalid offset: -1: invalid blob offset or length")},
		}

		var v gather.WriteBuffer
		defer v.Close()

		for _, tc := range cases {
			err := cc.GetContent(ctx, tc.contentID, tc.blobID, tc.offset, tc.length, &v)
			if tc.err == nil {
				require.NoErrorf(t, err, "tc.contentID: %v", tc.contentID)
			} else {
				require.ErrorContainsf(t, err, tc.err.Error(), "tc.contentID: %v", tc.contentID)
			}
			if got := v.ToByteSlice(); !bytes.Equal(got, tc.expected) {
				t.Errorf("unexpected data for %v: %x, wanted %x", tc.contentID, got, tc.expected)
			}
		}

		verifyStorageContentList(t, cacheStorage, "f0f0f1x", "f0f0f2x", "f0f0f5")
	})

	t.Run("DataCorruption", func(t *testing.T) {
		const cacheKey = "f0f0f1x"

		var tmp gather.WriteBuffer
		defer tmp.Close()

		require.NoError(t, cacheStorage.GetBlob(ctx, cacheKey, 0, -1, &tmp))

		// corrupt the data and write back
		b := tmp.Bytes()
		b.Slices[0][0] ^= 1

		require.NoError(t, cacheStorage.PutBlob(ctx, cacheKey, b, blob.PutOptions{}))

		err := cc.GetContent(ctx, "xf0f0f1", "content-1", 1, 5, &tmp)
		require.NoError(t, err, "error in getContent")

		got, want := tmp.ToByteSlice(), []byte{2, 3, 4, 5, 6}
		require.Equal(t, want, got, "invalid result when reading corrupted data")
	})
}

func TestCacheFailureToOpen(t *testing.T) {
	someError := errors.New("some error")

	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForContentCacheTesting(t)
	faultyCache := blobtesting.NewFaultyStorage(cacheStorage)
	faultyCache.AddFault(blobtesting.MethodGetMetadata).ErrorInstead(someError)

	// Will fail because of ListBlobs failure.
	_, err := cache.NewContentCache(testlogging.Context(t), underlyingStorage, cache.Options{
		Storage: withoutTouchBlob{faultyCache},
		Sweep:   cache.SweepSettings{MaxSizeBytes: 10000},
	}, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, someError.Error())

	// ListBlobs fails only once, next time it succeeds.
	ctx := testlogging.Context(t)

	cc, err := cache.NewContentCache(ctx, underlyingStorage, cache.Options{
		Storage: withoutTouchBlob{faultyCache},
		Sweep:   cache.SweepSettings{MaxSizeBytes: 10000},
	}, nil)
	require.NoError(t, err)

	cc.Close(ctx)
}

func TestCacheFailureToWrite(t *testing.T) {
	someError := errors.New("some error")

	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForContentCacheTesting(t)
	faultyCache := blobtesting.NewFaultyStorage(cacheStorage)

	cc, err := cache.NewContentCache(testlogging.Context(t), underlyingStorage, cache.Options{
		Storage: withoutTouchBlob{faultyCache},
		Sweep:   cache.SweepSettings{MaxSizeBytes: 10000},
	}, nil)
	require.NoError(t, err)

	ctx := testlogging.Context(t)

	defer cc.Close(ctx)

	faultyCache.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)

	var v gather.WriteBuffer
	defer v.Close()

	err = cc.GetContent(ctx, "aa", "content-1", 0, 3, &v)
	require.NoError(t, err, "write failure wasn't ignored")

	got, want := v.ToByteSlice(), []byte{1, 2, 3}
	require.Equal(t, want, got, "unexpected value retrieved from cache")

	all, err := blob.ListAllBlobs(ctx, cacheStorage, "")
	require.NoError(t, err, "error listing cache")

	require.Empty(t, all, "invalid test - cache was written")
}

func TestCacheFailureToRead(t *testing.T) {
	someError := errors.New("some error")

	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForContentCacheTesting(t)
	faultyCache := blobtesting.NewFaultyStorage(cacheStorage)

	cc, err := cache.NewContentCache(testlogging.Context(t), underlyingStorage, cache.Options{
		Storage: withoutTouchBlob{faultyCache},
		Sweep:   cache.SweepSettings{MaxSizeBytes: 10000},
	}, nil)
	require.NoError(t, err)

	ctx := testlogging.Context(t)

	defer cc.Close(ctx)

	faultyCache.AddFault(blobtesting.MethodGetBlob).ErrorInstead(someError).Repeat(100)

	var v gather.WriteBuffer
	defer v.Close()

	for range 2 {
		require.NoError(t, cc.GetContent(ctx, "aa", "content-1", 0, 3, &v))

		got, want := v.ToByteSlice(), []byte{1, 2, 3}
		assert.Equal(t, want, got, "unexpected value retrieved from cache")
	}
}

func verifyStorageContentList(t *testing.T, st blob.Storage, expectedContents ...blob.ID) {
	t.Helper()

	var foundContents []blob.ID

	require.NoError(t, st.ListBlobs(testlogging.Context(t), "", func(bm blob.Metadata) error {
		foundContents = append(foundContents, bm.BlobID)
		return nil
	}))

	sort.Slice(foundContents, func(i, j int) bool {
		return foundContents[i] < foundContents[j]
	})

	assert.Equal(t, expectedContents, foundContents, "unexpected content list")
}

type withoutTouchBlob struct {
	blob.Storage
}

func (c withoutTouchBlob) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) (time.Time, error) {
	return time.Time{}, errors.New("TouchBlob not implemented")
}
