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

	"github.com/kopia/kopia/internal/storagetesting"
	"github.com/kopia/kopia/repo/storage"
	"github.com/pkg/errors"
)

func newUnderlyingStorageForBlockCacheTesting(t *testing.T) storage.Storage {
	ctx := context.Background()
	data := map[string][]byte{}
	st := storagetesting.NewMapStorage(data, nil, nil)
	assertNoError(t, st.PutBlock(ctx, "block-1", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
	assertNoError(t, st.PutBlock(ctx, "block-4k", bytes.Repeat([]byte{1, 2, 3, 4}, 1000))) // 4000 bytes
	return st
}

func TestCacheExpiration(t *testing.T) {
	cacheData := map[string][]byte{}
	cacheStorage := storagetesting.NewMapStorage(cacheData, nil, nil)

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
	assertNoError(t, underlyingStorage.DeleteBlock(ctx, "block-4k"))

	cases := []struct {
		block         string
		expectedError error
	}{
		{"00000a", storage.ErrBlockNotFound},
		{"00000b", storage.ErrBlockNotFound},
		{"00000c", nil},
		{"00000d", nil},
	}

	for _, tc := range cases {
		_, got := cache.getContentBlock(ctx, tc.block, "block-4k", 0, -1)
		if want := tc.expectedError; got != want {
			t.Errorf("unexpected error when getting block %v: %v wanted %v", tc.block, got, want)
		} else {
			t.Logf("got correct error %v when reading block %v", tc.expectedError, tc.block)
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
			cacheKey        string
			physicalBlockID string
			offset          int64
			length          int64

			expected []byte
			err      error
		}{
			{"xf0f0f1", "block-1", 1, 5, []byte{2, 3, 4, 5, 6}, nil},
			{"xf0f0f2", "block-1", 0, -1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil},
			{"xf0f0f1", "block-1", 1, 5, []byte{2, 3, 4, 5, 6}, nil},
			{"xf0f0f2", "block-1", 0, -1, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil},
			{"xf0f0f3", "no-such-block", 0, -1, nil, storage.ErrBlockNotFound},
			{"xf0f0f4", "no-such-block", 10, 5, nil, storage.ErrBlockNotFound},
			{"f0f0f5", "block-1", 7, 3, []byte{8, 9, 10}, nil},
			{"xf0f0f6", "block-1", 11, 10, nil, errors.Errorf("invalid offset")},
			{"xf0f0f6", "block-1", -1, 5, nil, errors.Errorf("invalid offset")},
		}

		for _, tc := range cases {
			v, err := cache.getContentBlock(ctx, tc.cacheKey, tc.physicalBlockID, tc.offset, tc.length)
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
		cacheKey := "f0f0f1x"
		d, err := cache.cacheStorage.GetBlock(ctx, cacheKey, 0, -1)
		if err != nil {
			t.Fatalf("unable to retrieve data from cache: %v", err)
		}

		// corrupt the data and write back
		d[0] ^= 1

		if err := cache.cacheStorage.PutBlock(ctx, cacheKey, d); err != nil {
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

	cacheData := map[string][]byte{}
	cacheStorage := storagetesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForBlockCacheTesting(t)
	faultyCache := &storagetesting.FaultyStorage{
		Base: cacheStorage,
		Faults: map[string][]*storagetesting.Fault{
			"ListBlocks": {
				{Err: someError},
			},
		},
	}

	// Will fail because of ListBlocks failure.
	_, err := newBlockCacheWithCacheStorage(context.Background(), underlyingStorage, faultyCache, CachingOptions{
		MaxCacheSizeBytes: 10000,
	}, 0, 5*time.Hour)
	if err == nil || !strings.Contains(err.Error(), someError.Error()) {
		t.Errorf("invalid error %v, wanted: %v", err, someError)
	}

	// ListBlocks fails only once, next time it succeeds.
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

	cacheData := map[string][]byte{}
	cacheStorage := storagetesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForBlockCacheTesting(t)
	faultyCache := &storagetesting.FaultyStorage{
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
	faultyCache.Faults = map[string][]*storagetesting.Fault{
		"PutBlock": {
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

	all, err := storage.ListAllBlocks(ctx, cacheStorage, "")
	if err != nil {
		t.Errorf("error listing cache: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("invalid test - cache was written")
	}
}

func TestCacheFailureToRead(t *testing.T) {
	someError := errors.New("some error")

	cacheData := map[string][]byte{}
	cacheStorage := storagetesting.NewMapStorage(cacheData, nil, nil)
	underlyingStorage := newUnderlyingStorageForBlockCacheTesting(t)
	faultyCache := &storagetesting.FaultyStorage{
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
	faultyCache.Faults = map[string][]*storagetesting.Fault{
		"GetBlock": {
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

func verifyStorageBlockList(t *testing.T, st storage.Storage, expectedBlocks ...string) {
	t.Helper()
	var foundBlocks []string
	assertNoError(t, st.ListBlocks(context.Background(), "", func(bm storage.BlockMetadata) error {
		foundBlocks = append(foundBlocks, bm.BlockID)
		return nil
	}))

	sort.Strings(foundBlocks)
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
