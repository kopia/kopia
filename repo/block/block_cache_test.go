package block

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
)

func newUnderlyingStorageForBlockCacheTesting(t *testing.T) blob.Storage {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	assertNoError(t, st.PutBlob(ctx, "block-1", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
	assertNoError(t, st.PutBlob(ctx, "block-4k", bytes.Repeat([]byte{1, 2, 3, 4}, 1000))) // 4000 bytes
	return st
}

func TestCacheExpiration(t *testing.T) {
	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil)

	underlyingStorage := newUnderlyingStorageForBlockCacheTesting(t)

	cache, err := newBlockCacheWithCacheStorage(context.Background(), underlyingStorage, cacheStorage, CachingOptions{
		MaxCacheSizeBytes: 10000,
	}, 0, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer cache.close()

	ctx := context.Background()
	_, err = cache.getContentBlock(ctx, "00000a", "block-4k", 0, -1) // 4k
	assertNoError(t, err)
	_, err = cache.getContentBlock(ctx, "00000b", "block-4k", 0, -1) // 4k
	assertNoError(t, err)
	_, err = cache.getContentBlock(ctx, "00000c", "block-4k", 0, -1) // 4k
	assertNoError(t, err)
	_, err = cache.getContentBlock(ctx, "00000d", "block-4k", 0, -1) // 4k
	assertNoError(t, err)

	// wait for a sweep
	time.Sleep(2 * time.Second)

	// 00000a and 00000b will be removed from cache because it's the oldest.
	// to verify, let's remove block-4k from the underlying storage and make sure we can still read
	// 00000c and 00000d from the cache but not 00000a nor 00000b
	assertNoError(t, underlyingStorage.DeleteBlob(ctx, "block-4k"))

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
		_, got := cache.getContentBlock(ctx, tc.blobID, "block-4k", 0, -1)
		if want := tc.expectedError; got != want {
			t.Errorf("unexpected error when getting block %v: %v wanted %v", tc.blobID, got, want)
		} else {
			t.Logf("got correct error %v when reading block %v", tc.expectedError, tc.blobID)
		}
	}
}

func TestDiskBlockCache(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := ioutil.TempDir("", "kopia")
	if err != nil {
		t.Fatalf("error getting temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := newBlockCache(ctx, newUnderlyingStorageForBlockCacheTesting(t), CachingOptions{
		MaxCacheSizeBytes: 10000,
		CacheDirectory:    tmpDir,
	})

	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer cache.close()
	verifyBlockCache(t, cache)
}

func verifyBlockCache(t *testing.T, cache *blockCache) {
	ctx := context.Background()

	t.Run("GetContentBlock", func(t *testing.T) {
		cases := []struct {
			cacheKey blob.ID
			blobID   blob.ID
			offset   int64
			length   int64

			expected []byte
			err      error
		}{
			{"xf0f0f1", "block-1", 1, 5, []byte{2, 3, 4, 5, 6}, nil},
			{"xf0f0f2", "block-1", 0, -1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil},
			{"xf0f0f1", "block-1", 1, 5, []byte{2, 3, 4, 5, 6}, nil},
			{"xf0f0f2", "block-1", 0, -1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil},
			{"xf0f0f3", "no-such-block", 0, -1, nil, blob.ErrBlobNotFound},
			{"xf0f0f4", "no-such-block", 10, 5, nil, blob.ErrBlobNotFound},
			{"f0f0f5", "block-1", 7, 3, []byte{8, 9, 10}, nil},
			{"xf0f0f6", "block-1", 11, 10, nil, errors.Errorf("invalid offset")},
			{"xf0f0f6", "block-1", -1, 5, nil, errors.Errorf("invalid offset")},
		}

		for _, tc := range cases {
			v, err := cache.getContentBlock(ctx, tc.cacheKey, tc.blobID, tc.offset, tc.length)
			if (err != nil) != (tc.err != nil) {
				t.Errorf("unexpected error for %v: %+v, wanted %+v", tc.cacheKey, err, tc.err)
			} else if err != nil && err.Error() != tc.err.Error() {
				t.Errorf("unexpected error for %v: %+v, wanted %+v", tc.cacheKey, err, tc.err)
			}
			if !reflect.DeepEqual(v, tc.expected) {
				t.Errorf("unexpected data for %v: %x, wanted %x", tc.cacheKey, v, tc.expected)
			}
		}

		verifyStorageBlockList(t, cache.cacheStorage, "f0f0f1x", "f0f0f2x", "f0f0f5")
	})

	t.Run("DataCorruption", func(t *testing.T) {
		var cacheKey blob.ID = "f0f0f1x"
		d, err := cache.cacheStorage.GetBlob(ctx, cacheKey, 0, -1)
		if err != nil {
			t.Fatalf("unable to retrieve data from cache: %v", err)
		}

		// corrupt the data and write back
		d[0] ^= 1

		if err := cache.cacheStorage.PutBlob(ctx, cacheKey, d); err != nil {
			t.Fatalf("unable to write corrupted block: %v", err)
		}

		v, err := cache.getContentBlock(ctx, "xf0f0f1", "block-1", 1, 5)
		if err != nil {
			t.Fatalf("error in getContentBlock: %v", err)
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
	underlyingStorage := newUnderlyingStorageForBlockCacheTesting(t)
	faultyCache := &blobtesting.FaultyStorage{
		Base: cacheStorage,
		Faults: map[string][]*blobtesting.Fault{
			"ListBlobs": {
				{Err: someError},
			},
		},
	}

	// Will fail because of ListBlobs failure.
	_, err := newBlockCacheWithCacheStorage(context.Background(), underlyingStorage, faultyCache, CachingOptions{
		MaxCacheSizeBytes: 10000,
	}, 0, 5*time.Hour)
	if err == nil || !strings.Contains(err.Error(), someError.Error()) {
		t.Errorf("invalid error %v, wanted: %v", err, someError)
	}

	// ListBlobs fails only once, next time it succeeds.
	cache, err := newBlockCacheWithCacheStorage(context.Background(), underlyingStorage, faultyCache, CachingOptions{
		MaxCacheSizeBytes: 10000,
	}, 0, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	defer cache.close()
}

func TestCacheFailureToWrite(t *testing.T) {
	someError := errors.New("some error")

	cacheData := blobtesting.DataMap{}
	cacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForBlockCacheTesting(t)
	faultyCache := &blobtesting.FaultyStorage{
		Base: cacheStorage,
	}

	cache, err := newBlockCacheWithCacheStorage(context.Background(), underlyingStorage, faultyCache, CachingOptions{
		MaxCacheSizeBytes: 10000,
	}, 0, 5*time.Hour)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	defer cache.close()

	ctx := context.Background()
	faultyCache.Faults = map[string][]*blobtesting.Fault{
		"PutBlob": {
			{Err: someError},
		},
	}

	v, err := cache.getContentBlock(ctx, "aa", "block-1", 0, 3)
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
	underlyingStorage := newUnderlyingStorageForBlockCacheTesting(t)
	faultyCache := &blobtesting.FaultyStorage{
		Base: cacheStorage,
	}

	cache, err := newBlockCacheWithCacheStorage(context.Background(), underlyingStorage, faultyCache, CachingOptions{
		MaxCacheSizeBytes: 10000,
	}, 0, 5*time.Hour)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	defer cache.close()

	ctx := context.Background()
	faultyCache.Faults = map[string][]*blobtesting.Fault{
		"GetBlob": {
			{Err: someError, Repeat: 100},
		},
	}

	for i := 0; i < 2; i++ {
		v, err := cache.getContentBlock(ctx, "aa", "block-1", 0, 3)
		if err != nil {
			t.Errorf("read failure wasn't ignored: %v", err)
		}

		if got, want := v, []byte{1, 2, 3}; !reflect.DeepEqual(got, want) {
			t.Errorf("unexpected value retrieved from cache: %v, want: %v", got, want)
		}
	}
}

func verifyStorageBlockList(t *testing.T, st blob.Storage, expectedBlocks ...blob.ID) {
	t.Helper()
	var foundBlocks []blob.ID
	assertNoError(t, st.ListBlobs(context.Background(), "", func(bm blob.Metadata) error {
		foundBlocks = append(foundBlocks, bm.BlobID)
		return nil
	}))

	sort.Slice(foundBlocks, func(i, j int) bool {
		return foundBlocks[i] < foundBlocks[j]
	})
	if !reflect.DeepEqual(foundBlocks, expectedBlocks) {
		t.Errorf("unexpected block list: %v, wanted %v", foundBlocks, expectedBlocks)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("err: %v", err)
	}
}
