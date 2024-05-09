package cache_test

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
)

type newContentCacheFunc func(ctx context.Context, st blob.Storage, cacheStorage cache.Storage) (cache.ContentCache, error)

func newContentDataCache(ctx context.Context, st blob.Storage, cacheStorage cache.Storage) (cache.ContentCache, error) {
	return cache.NewContentCache(ctx, st, cache.Options{
		Storage:    cacheStorage,
		HMACSecret: []byte{1, 2, 3, 4},
		Sweep: cache.SweepSettings{
			MaxSizeBytes: 100,
		},
	}, nil)
}

func newContentMetadataCache(ctx context.Context, st blob.Storage, cacheStorage cache.Storage) (cache.ContentCache, error) {
	return cache.NewContentCache(ctx, st, cache.Options{
		Storage:        cacheStorage,
		HMACSecret:     []byte{1, 2, 3, 4},
		FetchFullBlobs: true,
		Sweep: cache.SweepSettings{
			MaxSizeBytes: 100,
		},
	}, nil)
}

func TestPrefetchBlocksGetContent_DataCache(t *testing.T) {
	t.Parallel()
	testContentCachePrefetchBlocksGetContent(t, newContentDataCache)
}

func TestPrefetchBlocksGetContent_MetadataCache(t *testing.T) {
	t.Parallel()
	testContentCachePrefetchBlocksGetContent(t, newContentMetadataCache)
}

func TestGetContentForDifferentContentIDsExecutesInParallel_DataCache(t *testing.T) {
	t.Parallel()
	testGetContentForDifferentContentIDsExecutesInParallel(t, newContentDataCache, 2)
}

func TestGetContentForDifferentContentIDsExecutesInParallel_MetadataCache(t *testing.T) {
	t.Parallel()
	testGetContentForDifferentContentIDsExecutesInParallel(t, newContentMetadataCache, 1)
}

func TestGetContentForDifferentBlobsExecutesInParallel_DataCache(t *testing.T) {
	t.Parallel()
	testGetContentForDifferentBlobsExecutesInParallel(t, newContentDataCache)
}

func TestGetContentForDifferentBlobsExecutesInParallel_MetadataCache(t *testing.T) {
	t.Parallel()
	testGetContentForDifferentBlobsExecutesInParallel(t, newContentMetadataCache)
}

func TestGetContentRaceFetchesOnce_DataCache(t *testing.T) {
	t.Parallel()
	testGetContentRaceFetchesOnce(t, newContentDataCache)
}

func TestGetContentRaceFetchesOnce_MetadataCache(t *testing.T) {
	testGetContentRaceFetchesOnce(t, newContentMetadataCache)
}

//nolint:thelper
func testContentCachePrefetchBlocksGetContent(t *testing.T, newCache newContentCacheFunc) {
	ctx := testlogging.Context(t)

	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)
	faulty := blobtesting.NewFaultyStorage(underlying)

	cacheData := blobtesting.DataMap{}
	metadataCacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil).(cache.Storage)

	dataCache, err := newCache(ctx, faulty, metadataCacheStorage)
	require.NoError(t, err)

	defer dataCache.Close(ctx)

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.NoError(t, underlying.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6}), blob.PutOptions{}))

	getBlobStarted := make(chan struct{})

	var (
		wg sync.WaitGroup

		counter               = new(int32)
		getBlobFinishedCnt    int32
		getContentFinishedCnt int32
	)

	wg.Add(1)

	go func() {
		var tmp gather.WriteBuffer
		defer tmp.Close()
		defer wg.Done()

		<-getBlobStarted // wait until underlying blob starts being fetched

		// start reading content while this is ongoing
		t.Logf("GetContent started")
		dataCache.GetContent(ctx, "c1", "blob1", 0, 1, &tmp)
		t.Logf("GetContent finished")

		getContentFinishedCnt = atomic.AddInt32(counter, 1)
	}()

	faulty.AddFault(blobtesting.MethodGetBlob).Before(func() {
		t.Logf("GetBlob started")
		close(getBlobStarted)
		time.Sleep(500 * time.Millisecond)
		t.Logf("GetBlob finished")
	})

	faulty.AddFault(blobtesting.MethodGetBlob).Before(func() {
		t.Error("will not be called")
	})

	dataCache.PrefetchBlob(ctx, "blob1")
	dataCache.PrefetchBlob(ctx, "blob1")

	wg.Wait()

	// ensure getBlob finishes before getContent finishes despite GetBlob taking non-trivial time
	require.Less(t, getBlobFinishedCnt, getContentFinishedCnt)
}

//nolint:thelper
func testGetContentForDifferentContentIDsExecutesInParallel(t *testing.T, newCache newContentCacheFunc, minGetBlobParallelism int) {
	ctx := testlogging.Context(t)

	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)
	faulty := blobtesting.NewFaultyStorage(underlying)

	cacheData := blobtesting.DataMap{}
	metadataCacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil).(cache.Storage)

	dataCache, err := newCache(ctx, faulty, metadataCacheStorage)
	require.NoError(t, err)

	defer dataCache.Close(ctx)

	require.NoError(t, underlying.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), blob.PutOptions{}))

	var ct concurrencyTester

	faulty.AddFault(blobtesting.MethodGetBlob).Before(func() {
		ct.enter()
		time.Sleep(time.Second)
		ct.exit()
	}).Repeat(100)

	var wg sync.WaitGroup

	for i := range 20 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var tmp gather.WriteBuffer
			defer tmp.Close()

			dataCache.GetContent(ctx, fmt.Sprintf("c%v", i), "blob1", int64(i), 1, &tmp)
		}()
	}

	wg.Wait()

	require.GreaterOrEqual(t, ct.maxConcurrencyLevel, minGetBlobParallelism)
}

//nolint:thelper
func testGetContentForDifferentBlobsExecutesInParallel(t *testing.T, newCache newContentCacheFunc) {
	ctx := testlogging.Context(t)

	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)
	faulty := blobtesting.NewFaultyStorage(underlying)

	cacheData := blobtesting.DataMap{}
	metadataCacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil).(cache.Storage)

	dataCache, err := newCache(ctx, faulty, metadataCacheStorage)
	require.NoError(t, err)

	defer dataCache.Close(ctx)

	for i := range 100 {
		require.NoError(t, underlying.PutBlob(ctx, blob.ID(fmt.Sprintf("blob%v", i)), gather.FromSlice([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), blob.PutOptions{}))
	}

	var ct concurrencyTester

	faulty.AddFault(blobtesting.MethodGetBlob).Before(func() {
		ct.enter()
		time.Sleep(time.Second)
		ct.exit()
	}).Repeat(100)

	var wg sync.WaitGroup

	for i := range 20 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var tmp gather.WriteBuffer
			defer tmp.Close()

			dataCache.GetContent(ctx, fmt.Sprintf("c%v", i), blob.ID(fmt.Sprintf("blob%v", i)), int64(i), 1, &tmp)
		}()
	}

	wg.Wait()

	require.GreaterOrEqual(t, ct.maxConcurrencyLevel, 2)
}

//nolint:thelper
func testGetContentRaceFetchesOnce(t *testing.T, newCache newContentCacheFunc) {
	ctx := testlogging.Context(t)

	underlyingData := blobtesting.DataMap{}
	underlying := blobtesting.NewMapStorage(underlyingData, nil, nil)
	faulty := blobtesting.NewFaultyStorage(underlying)

	cacheData := blobtesting.DataMap{}
	metadataCacheStorage := blobtesting.NewMapStorage(cacheData, nil, nil).(cache.Storage)

	dataCache, err := newCache(ctx, faulty, metadataCacheStorage)
	require.NoError(t, err)

	defer dataCache.Close(ctx)

	require.NoError(t, underlying.PutBlob(ctx, "blob1", gather.FromSlice([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), blob.PutOptions{}))

	faulty.AddFault(blobtesting.MethodGetBlob).Before(func() {
		time.Sleep(time.Second)
	})

	// this should not execute
	faulty.AddFault(blobtesting.MethodGetBlob).Before(func() {
		t.Errorf("GetBlob was called more than once - stack2: %s", debug.Stack())
	})

	var wg sync.WaitGroup

	for range 20 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var tmp gather.WriteBuffer
			defer tmp.Close()

			dataCache.GetContent(ctx, "c1", "blob1", 0, 1, &tmp)
		}()
	}

	wg.Wait()
}

type concurrencyTester struct {
	mu sync.Mutex

	// +checklocks:mu
	concurrencyLevel int

	maxConcurrencyLevel int // +checklocksignore
}

func (c *concurrencyTester) enter() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.concurrencyLevel++
	if c.concurrencyLevel > c.maxConcurrencyLevel {
		c.maxConcurrencyLevel = c.concurrencyLevel
	}
}

func (c *concurrencyTester) exit() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.concurrencyLevel--
}
