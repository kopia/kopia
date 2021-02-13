package content

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobtesting"
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
	assertNoError(t, st.PutBlob(ctx, "content-1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})))
	assertNoError(t, st.PutBlob(ctx, "content-4k", gather.FromSlice(bytes.Repeat([]byte{1, 2, 3, 4}, 1000)))) // 4000 bytes

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

	cb, err := newContentCacheBase(testlogging.Context(t), "test cache", cacheStorage, 10000, 0, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("unable to create base cache: %v", err)
	}

	cache := &contentCacheForData{
		st:        underlyingStorage,
		cacheBase: cb,
	}

	ctx := testlogging.Context(t)

	defer cache.close(ctx)

	_, err = cache.getContent(ctx, "00000a", "content-4k", 0, -1) // 4k
	assertNoError(t, err)
	_, err = cache.getContent(ctx, "00000b", "content-4k", 0, -1) // 4k
	assertNoError(t, err)
	_, err = cache.getContent(ctx, "00000c", "content-4k", 0, -1) // 4k
	assertNoError(t, err)
	_, err = cache.getContent(ctx, "00000d", "content-4k", 0, -1) // 4k
	assertNoError(t, err)

	// wait for a sweep
	time.Sleep(2 * time.Second)

	// 00000a and 00000b will be removed from cache because it's the oldest.
	// to verify, let's remove content-4k from the underlying storage and make sure we can still read
	// 00000c and 00000d from the cache but not 00000a nor 00000b
	assertNoError(t, underlyingStorage.DeleteBlob(ctx, "content-4k"))

	cases := []struct {
		blobID        blob.ID
		expectedError error
	}{
		{"00000a", blob.ErrBlobNotFound},
		{"00000b", blob.ErrBlobNotFound},
		{"00000c", nil},
		{"00000d", nil},
	}

	for _, tc := range cases {
		_, got := cache.getContent(ctx, cacheKey(tc.blobID), "content-4k", 0, -1)
		if want := tc.expectedError; !errors.Is(got, want) {
			t.Errorf("unexpected error when getting content %v: %v wanted %v", tc.blobID, got, want)
		} else {
			t.Logf("got correct error %v when reading content %v", tc.expectedError, tc.blobID)
		}
	}
}

func TestDiskContentCache(t *testing.T) {
	ctx := testlogging.Context(t)

	tmpDir := testutil.TempDirectory(t)

	const maxBytes = 10000

	cacheStorage, err := newCacheStorageOrNil(ctx, tmpDir, maxBytes, "contents")
	if err != nil {
		t.Fatal(err)
	}

	cache, err := newContentCacheForData(ctx, newUnderlyingStorageForContentCacheTesting(t), cacheStorage, maxBytes, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	defer cache.close(ctx)

	verifyContentCache(t, cache)
}

func verifyContentCache(t *testing.T, cache contentCache) {
	t.Helper()

	ctx := testlogging.Context(t)

	t.Run("GetContentContent", func(t *testing.T) {
		cases := []struct {
			cacheKey cacheKey
			blobID   blob.ID
			offset   int64
			length   int64

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
			{"xf0f0f6", "content-1", 11, 10, nil, errors.Errorf("error getting content from cache: invalid offset: 11")},
			{"xf0f0f6", "content-1", -1, 5, nil, errors.Errorf("error getting content from cache: invalid offset: -1")},
		}

		for _, tc := range cases {
			v, err := cache.getContent(ctx, tc.cacheKey, tc.blobID, tc.offset, tc.length)
			if (err != nil) != (tc.err != nil) {
				t.Errorf("unexpected error for %v: %+v, wanted %+v", tc.cacheKey, err, tc.err)
			} else if err != nil && err.Error() != tc.err.Error() {
				t.Errorf("unexpected error for %v: %q, wanted %q", tc.cacheKey, err.Error(), tc.err.Error())
			}
			if !bytes.Equal(v, tc.expected) {
				t.Errorf("unexpected data for %v: %x, wanted %x", tc.cacheKey, v, tc.expected)
			}
		}

		verifyStorageContentList(t, cache.(*contentCacheForData).cacheStorage, "f0f0f1x", "f0f0f2x", "f0f0f5")
	})

	t.Run("DataCorruption", func(t *testing.T) {
		var cacheKey blob.ID = "f0f0f1x"
		d, err := cache.(*contentCacheForData).cacheStorage.GetBlob(ctx, cacheKey, 0, -1)
		if err != nil {
			t.Fatalf("unable to retrieve data from cache: %v", err)
		}

		// corrupt the data and write back
		d[0] ^= 1

		if puterr := cache.(*contentCacheForData).cacheStorage.PutBlob(ctx, cacheKey, gather.FromSlice(d)); puterr != nil {
			t.Fatalf("unable to write corrupted content: %v", puterr)
		}

		v, err := cache.getContent(ctx, "xf0f0f1", "content-1", 1, 5)
		if err != nil {
			t.Fatalf("error in getContent: %v", err)
		}
		if got, want := v, []byte{2, 3, 4, 5, 6}; !reflect.DeepEqual(v, want) {
			t.Errorf("invalid result when reading corrupted data: %v, wanted %v", got, want)
		}
	})
}

func TestCacheFailureToOpen(t *testing.T) {
	someError := errors.New("some error")

	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForContentCacheTesting(t)
	faultyCache := &blobtesting.FaultyStorage{
		Base: cacheStorage,
		Faults: map[string][]*blobtesting.Fault{
			"ListBlobs": {
				{Err: someError},
			},
		},
	}

	// Will fail because of ListBlobs failure.
	_, err := newContentCacheForData(testlogging.Context(t), underlyingStorage, faultyCache, 10000, nil)
	if err == nil || !strings.Contains(err.Error(), someError.Error()) {
		t.Errorf("invalid error %v, wanted: %v", err, someError)
	}

	// ListBlobs fails only once, next time it succeeds.
	ctx := testlogging.Context(t)

	cache, err := newContentCacheForData(ctx, underlyingStorage, faultyCache, 10000, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	defer cache.close(ctx)
}

func TestCacheFailureToWrite(t *testing.T) {
	someError := errors.New("some error")

	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForContentCacheTesting(t)
	faultyCache := &blobtesting.FaultyStorage{
		Base: cacheStorage,
	}

	cache, err := newContentCacheForData(testlogging.Context(t), underlyingStorage, faultyCache, 10000, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ctx := testlogging.Context(t)

	defer cache.close(ctx)

	faultyCache.Faults = map[string][]*blobtesting.Fault{
		"PutBlob": {
			{Err: someError},
		},
	}

	v, err := cache.getContent(ctx, "aa", "content-1", 0, 3)
	if err != nil {
		t.Errorf("write failure wasn't ignored: %v", err)
	}

	if got, want := v, []byte{1, 2, 3}; !reflect.DeepEqual(got, want) {
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
	faultyCache := &blobtesting.FaultyStorage{
		Base: cacheStorage,
	}

	cache, err := newContentCacheForData(testlogging.Context(t), underlyingStorage, faultyCache, 10000, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	ctx := testlogging.Context(t)

	defer cache.close(ctx)

	faultyCache.Faults = map[string][]*blobtesting.Fault{
		"GetBlob": {
			{Err: someError, Repeat: 100},
		},
	}

	for i := 0; i < 2; i++ {
		v, err := cache.getContent(ctx, "aa", "content-1", 0, 3)
		if err != nil {
			t.Errorf("read failure wasn't ignored: %v", err)
		}

		if got, want := v, []byte{1, 2, 3}; !reflect.DeepEqual(got, want) {
			t.Errorf("unexpected value retrieved from cache: %v, want: %v", got, want)
		}
	}
}

func verifyStorageContentList(t *testing.T, st blob.Storage, expectedContents ...blob.ID) {
	t.Helper()

	var foundContents []blob.ID

	assertNoError(t, st.ListBlobs(testlogging.Context(t), "", func(bm blob.Metadata) error {
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

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}
