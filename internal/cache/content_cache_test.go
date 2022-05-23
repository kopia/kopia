package cache_test

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

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

func TestCacheExpiration(t *testing.T) {
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
		Sweep: cache.SweepSettings{
			MaxSizeBytes:   10000,
			SweepFrequency: 500 * time.Millisecond,
			TouchThreshold: -1,
		},
	})

	require.NoError(t, err)

	defer cc.Close(ctx)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	err = cc.GetContent(ctx, "00000a", "content-4k", 0, -1, &tmp) // 4k
	require.NoError(t, err)
	err = cc.GetContent(ctx, "00000b", "content-4k", 0, -1, &tmp) // 4k
	require.NoError(t, err)
	err = cc.GetContent(ctx, "00000c", "content-4k", 0, -1, &tmp) // 4k
	require.NoError(t, err)
	err = cc.GetContent(ctx, "00000d", "content-4k", 0, -1, &tmp) // 4k
	require.NoError(t, err)

	// wait for a sweep
	time.Sleep(2 * time.Second)

	// 00000a and 00000b will be removed from cache because it's the oldest.
	// to verify, let's remove content-4k from the underlying storage and make sure we can still read
	// 00000c and 00000d from the cache but not 00000a nor 00000b
	require.NoError(t, underlyingStorage.DeleteBlob(ctx, "content-4k"))

	cases := []struct {
		contentID     string
		expectedError error
	}{
		{"00000a", blob.ErrBlobNotFound},
		{"00000b", blob.ErrBlobNotFound},
		{"00000c", nil},
		{"00000d", nil},
	}

	for _, tc := range cases {
		got := cc.GetContent(ctx, tc.contentID, "content-4k", 0, -1, &tmp)
		if want := tc.expectedError; !errors.Is(got, want) {
			t.Errorf("unexpected error when getting content %v: %v wanted %v", tc.contentID, got, want)
		} else {
			t.Logf("got correct error %v when reading content %v", tc.expectedError, tc.contentID)
		}
	}
}

func TestDiskContentCache(t *testing.T) {
	ctx := testlogging.Context(t)

	tmpDir := testutil.TempDirectory(t)

	const maxBytes = 10000

	cacheStorage, err := cache.NewStorageOrNil(ctx, tmpDir, maxBytes, "contents")
	if err != nil {
		t.Fatal(err)
	}

	cc, err := cache.NewContentCache(ctx, newUnderlyingStorageForContentCacheTesting(t), cache.Options{
		Storage: cacheStorage,
		Sweep: cache.SweepSettings{
			MaxSizeBytes: maxBytes,
		},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

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
			{"xf0f0f6", "content-1", 11, 10, nil, errors.Errorf("invalid offset: 11: invalid blob offset or length")},
			{"xf0f0f6", "content-1", -1, 5, nil, errors.Errorf("invalid offset: -1: invalid blob offset or length")},
		}

		var v gather.WriteBuffer
		defer v.Close()

		for _, tc := range cases {
			err := cc.GetContent(ctx, tc.contentID, tc.blobID, tc.offset, tc.length, &v)
			if tc.err == nil {
				require.NoErrorf(t, err, "tc.contentID=%v", tc.contentID)
			} else {
				require.ErrorContainsf(t, err, tc.err.Error(), "tc.contentID=%v", tc.contentID)
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
		if err != nil {
			t.Fatalf("error in getContent: %v", err)
		}

		if got, want := tmp.ToByteSlice(), []byte{2, 3, 4, 5, 6}; !reflect.DeepEqual(got, want) {
			t.Errorf("invalid result when reading corrupted data: %v, wanted %v", got, want)
		}
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
	})
	if err == nil || !strings.Contains(err.Error(), someError.Error()) {
		t.Errorf("invalid error %v, wanted: %v", err, someError)
	}

	// ListBlobs fails only once, next time it succeeds.
	ctx := testlogging.Context(t)

	cc, err := cache.NewContentCache(ctx, underlyingStorage, cache.Options{
		Storage: withoutTouchBlob{faultyCache},
		Sweep:   cache.SweepSettings{MaxSizeBytes: 10000},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

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
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ctx := testlogging.Context(t)

	defer cc.Close(ctx)

	faultyCache.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)

	var v gather.WriteBuffer
	defer v.Close()

	if err = cc.GetContent(ctx, "aa", "content-1", 0, 3, &v); err != nil {
		t.Errorf("write failure wasn't ignored: %v", err)
	}

	if got, want := v.ToByteSlice(), []byte{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected value retrieved from cache: %v, want: %v", got, want)
	}

	all, err := blob.ListAllBlobs(ctx, cacheStorage, "")
	if err != nil {
		t.Errorf("error listing cache: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("invalid test - cache was written")
	}
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
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ctx := testlogging.Context(t)

	defer cc.Close(ctx)

	faultyCache.AddFault(blobtesting.MethodGetBlob).ErrorInstead(someError).Repeat(100)

	var v gather.WriteBuffer
	defer v.Close()

	for i := 0; i < 2; i++ {
		require.NoError(t, cc.GetContent(ctx, "aa", "content-1", 0, 3, &v))

		if got, want := v.ToByteSlice(), []byte{1, 2, 3}; !reflect.DeepEqual(got, want) {
			t.Errorf("unexpected value retrieved from cache: %v, want: %v", got, want)
		}
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

	if !reflect.DeepEqual(foundContents, expectedContents) {
		t.Errorf("unexpected content list: %v, wanted %v", foundContents, expectedContents)
	}
}

type withoutTouchBlob struct {
	blob.Storage
}

func (c withoutTouchBlob) TouchBlob(ctx context.Context, blobID blob.ID, threshold time.Duration) error {
	return errors.Errorf("TouchBlob not implemented")
}
