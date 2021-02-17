package content

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/blob"
)

const (
	defaultSweepFrequency = 1 * time.Minute
	defaultTouchThreshold = 10 * time.Minute
	mutexAgeCutoff        = 5 * time.Minute
)

type mutexLRU struct {
	// values aligned to 8-bytes due to atomic access
	lastUsedNanoseconds int64

	mu *sync.Mutex
}

// cacheBase provides common implementation for per-content and per-blob caches.
type cacheBase struct {
	anyChange int32

	cacheStorage   blob.Storage
	maxSizeBytes   int64
	sweepFrequency time.Duration
	touchThreshold time.Duration
	description    string

	periodicSweepRunning sync.WaitGroup
	periodicSweepClosed  chan struct{}

	// stores key to *mutexLRU mapping which is periodically garbage-collected
	// and used to eliminate/minimize concurrent fetches of the same cached item.
	loadingMap sync.Map
}

type blobToucher interface {
	TouchBlob(ctx context.Context, contentID blob.ID, threshold time.Duration) error
}

func (c *cacheBase) touch(ctx context.Context, blobID blob.ID) {
	if t, ok := c.cacheStorage.(blobToucher); ok {
		t.TouchBlob(ctx, blobID, c.touchThreshold) //nolint:errcheck
	}
}

func (c *cacheBase) close(ctx context.Context) {
	close(c.periodicSweepClosed)
	c.periodicSweepRunning.Wait()

	// if we added anything to the cache in this sesion, run one last sweep before shutting down.
	if atomic.LoadInt32(&c.anyChange) == 1 {
		if err := c.sweepDirectory(ctx); err != nil {
			log(ctx).Warningf("error during final sweep of the %v: %v", c.description, err)
		}
	}
}

func (c *cacheBase) perItemMutex(key interface{}) *sync.Mutex {
	now := clock.Now().UnixNano()

	v, ok := c.loadingMap.Load(key)
	if !ok {
		v, _ = c.loadingMap.LoadOrStore(key, &mutexLRU{
			mu:                  &sync.Mutex{},
			lastUsedNanoseconds: now,
		})
	}

	m := v.(*mutexLRU)
	atomic.StoreInt64(&m.lastUsedNanoseconds, now)

	return m.mu
}

func (c *cacheBase) sweepDirectoryPeriodically(ctx context.Context) {
	defer c.periodicSweepRunning.Done()

	for {
		select {
		case <-c.periodicSweepClosed:
			return

		case <-time.After(c.sweepFrequency):
			c.sweepMutexes()

			if err := c.sweepDirectory(ctx); err != nil {
				log(ctx).Warningf("error during periodic sweep of %v: %v", c.description, err)
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
	t0 := clock.Now()

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
		return errors.Wrapf(err, "error listing %v", c.description)
	}

	log(ctx).Debugf("finished sweeping %v in %v and retained %v/%v bytes (%v %%)", c.description, clock.Since(t0), totalRetainedSize, c.maxSizeBytes, 100*totalRetainedSize/c.maxSizeBytes)

	return nil
}

func (c *cacheBase) sweepMutexes() {
	cutoffTime := clock.Now().Add(-mutexAgeCutoff).UnixNano()

	// remove from loadingMap all items that have not been touched recently.
	// since the mutexes are only for performance (to avoid loading duplicates)
	// and not for correctness, it's always safe to remove them.
	c.loadingMap.Range(func(key, value interface{}) bool {
		if m := value.(*mutexLRU); atomic.LoadInt64(&m.lastUsedNanoseconds) < cutoffTime {
			c.loadingMap.Delete(key)
		}

		return true
	})
}

func newContentCacheBase(ctx context.Context, description string, cacheStorage blob.Storage, maxSizeBytes int64, touchThreshold, sweepFrequency time.Duration) (*cacheBase, error) {
	c := &cacheBase{
		cacheStorage:        cacheStorage,
		maxSizeBytes:        maxSizeBytes,
		periodicSweepClosed: make(chan struct{}),
		touchThreshold:      touchThreshold,
		sweepFrequency:      sweepFrequency,
		description:         description,
	}

	// errGood is a marker error to stop blob iteration quickly, does not
	// indicate any problem.
	errGood := errors.Errorf("good")

	// verify that cache storage is functional by listing from it
	if err := c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		// nolint:wrapcheck
		return errGood
	}); err != nil && !errors.Is(err, errGood) {
		return nil, errors.Wrapf(err, "unable to open %v", c.description)
	}

	c.periodicSweepRunning.Add(1)

	go c.sweepDirectoryPeriodically(ctx)

	return c, nil
}
