package content

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const (
	defaultSweepFrequency = 1 * time.Minute
	defaultTouchThreshold = 10 * time.Minute
	mutexAgeCutoff        = 5 * time.Minute
)

type mutextLRU struct {
	mu                  *sync.Mutex
	lastUsedNanoseconds int64
}

// cacheBase provides common implementation for per-content and per-blob caches
type cacheBase struct {
	cacheStorage   blob.Storage
	maxSizeBytes   int64
	sweepFrequency time.Duration
	touchThreshold time.Duration

	asyncWG sync.WaitGroup
	closed  chan struct{}

	// stores key to *mutexLRU mapping which is periodically garbage-collected
	// and used to eliminate/minimize concurrent fetches of the same cached item.
	loadingMap sync.Map
}

type contentToucher interface {
	TouchBlob(ctx context.Context, contentID blob.ID, threshold time.Duration) error
}

func (c *cacheBase) touch(ctx context.Context, blobID blob.ID) {
	if t, ok := c.cacheStorage.(contentToucher); ok {
		t.TouchBlob(ctx, blobID, c.touchThreshold) //nolint:errcheck
	}
}

func (c *cacheBase) close() {
	close(c.closed)
	c.asyncWG.Wait()
}

func (c *cacheBase) perItemMutex(key interface{}) *sync.Mutex {
	now := time.Now().UnixNano() // allow:no-inject-time

	v, ok := c.loadingMap.Load(key)
	if !ok {
		v, _ = c.loadingMap.LoadOrStore(key, &mutextLRU{
			mu:                  &sync.Mutex{},
			lastUsedNanoseconds: now,
		})
	}

	m := v.(*mutextLRU)
	atomic.StoreInt64(&m.lastUsedNanoseconds, now)

	return m.mu
}

func (c *cacheBase) sweepDirectoryPeriodically(ctx context.Context) {
	defer c.asyncWG.Done()

	for {
		select {
		case <-c.closed:
			return

		case <-time.After(c.sweepFrequency):
			c.sweepMutexes()

			if err := c.sweepDirectory(ctx); err != nil {
				log(ctx).Warningf("cache sweep failed: %v", err)
			}
		}
	}
}

// A contentMetadataHeap implements heap.Interface and holds blob.Metadata.
type contentMetadataHeap []blob.Metadata

func (h contentMetadataHeap) Len() int { return len(h) }

func (h contentMetadataHeap) Less(i, j int) bool {
	return h[i].Timestamp.Before(h[j].Timestamp)
}

func (h contentMetadataHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *contentMetadataHeap) Push(x interface{}) {
	*h = append(*h, x.(blob.Metadata))
}

func (h *contentMetadataHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]

	return item
}

func (c *cacheBase) sweepDirectory(ctx context.Context) (err error) {
	t0 := time.Now() // allow:no-inject-time

	var h contentMetadataHeap

	var totalRetainedSize int64

	err = c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		heap.Push(&h, it)
		totalRetainedSize += it.Length

		if totalRetainedSize > c.maxSizeBytes {
			oldest := heap.Pop(&h).(blob.Metadata)
			if delerr := c.cacheStorage.DeleteBlob(ctx, oldest.BlobID); delerr != nil {
				log(ctx).Warningf("unable to remove %v: %v", oldest.BlobID, delerr)
			} else {
				totalRetainedSize -= oldest.Length
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "error listing cache")
	}

	log(ctx).Debugf("finished sweeping directory in %v and retained %v/%v bytes (%v %%)", time.Since(t0), totalRetainedSize, c.maxSizeBytes, 100*totalRetainedSize/c.maxSizeBytes) // allow:no-inject-time

	return nil
}

func (c *cacheBase) sweepMutexes() {
	cutoffTime := time.Now().Add(-mutexAgeCutoff).UnixNano() // allow:no-inject-time

	// remove from loadingMap all items that have not been touched recently.
	// since the mutexes are only for performance (to avoid loading duplicates)
	// and not for correctness, it's always safe to remove them.
	c.loadingMap.Range(func(key, value interface{}) bool {
		if m := value.(*mutextLRU); m.lastUsedNanoseconds < cutoffTime {
			c.loadingMap.Delete(key)
		}

		return true
	})
}

func newContentCacheBase(ctx context.Context, cacheStorage blob.Storage, maxSizeBytes int64, touchThreshold, sweepFrequency time.Duration) (*cacheBase, error) {
	c := &cacheBase{
		cacheStorage:   cacheStorage,
		maxSizeBytes:   maxSizeBytes,
		closed:         make(chan struct{}),
		touchThreshold: touchThreshold,
		sweepFrequency: sweepFrequency,
	}

	// errGood is a marker error to stop blob iteration quickly, does not
	// indicate any problem.
	var errGood = errors.Errorf("good")

	// verify that cache storage is functional by listing from it
	if err := c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		return errGood
	}); err != nil && err != errGood {
		return nil, errors.Wrap(err, "unable to open cache")
	}

	c.asyncWG.Add(1)

	go c.sweepDirectoryPeriodically(ctx)

	return c, nil
}
