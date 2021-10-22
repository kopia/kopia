// Package cache implements durable on-disk cache with LRU expiration.
package cache

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("cache")

const (
	// DefaultSweepFrequency is how frequently the contents of cache are sweeped to remove excess data.
	DefaultSweepFrequency = 1 * time.Minute

	// DefaultTouchThreshold specifies the resolution of timestamps used to determine which cache items
	// to expire. This helps cache storage writes on frequently accessed items.
	DefaultTouchThreshold = 10 * time.Minute
)

// PersistentCache provides persistent on-disk cache.
type PersistentCache struct {
	anyChange int32

	cacheStorage      Storage
	storageProtection StorageProtection

	maxSizeBytes   int64
	sweepFrequency time.Duration
	touchThreshold time.Duration
	description    string

	periodicSweepRunning sync.WaitGroup
	periodicSweepClosed  chan struct{}
}

// GetOrLoad is utility function gets the provided item from the cache or invokes the provided fetch function.
// The function also appends and verifies HMAC checksums using provided secret on all cached items to ensure data integrity.
func (c *PersistentCache) GetOrLoad(ctx context.Context, key string, fetch func(output *gather.WriteBuffer) error, output *gather.WriteBuffer) error {
	if c == nil {
		// special case - also works on non-initialized cache pointer.
		return fetch(output)
	}

	if c.Get(ctx, key, 0, -1, output) {
		return nil
	}

	output.Reset()

	if err := fetch(output); err != nil {
		stats.Record(ctx, MetricMissErrors.M(1))

		return err
	}

	stats.Record(ctx, MetricMissBytes.M(int64(output.Length())))

	c.Put(ctx, key, output.Bytes())

	return nil
}

// Get fetches the contents of a cached blob when (length < 0) or a subset of it (when length >= 0).
// returns nil if not found.
func (c *PersistentCache) Get(ctx context.Context, key string, offset, length int64, output *gather.WriteBuffer) bool {
	if c == nil {
		return false
	}

	if length >= 0 && !c.storageProtection.SupportsPartial() {
		return false
	}

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := c.cacheStorage.GetBlob(ctx, blob.ID(key), offset, length, &tmp); err == nil {
		if err := c.storageProtection.Verify(key, tmp.Bytes(), output); err == nil {
			// cache hit
			stats.Record(ctx,
				MetricHitCount.M(1),
				MetricHitBytes.M(int64(output.Length())),
			)

			// cache hit
			c.cacheStorage.TouchBlob(ctx, blob.ID(key), c.touchThreshold) //nolint:errcheck

			return true
		}

		// delete invalid blob
		stats.Record(ctx, MetricMalformedCacheDataCount.M(1))

		if err := c.cacheStorage.DeleteBlob(ctx, blob.ID(key)); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			log(ctx).Errorf("unable to delete %v entry %v: %v", c.description, key, err)
		}
	}

	// cache miss
	stats.Record(ctx, MetricMissCount.M(1))

	return false
}

// Put adds the provided key-value pair to the cache.
func (c *PersistentCache) Put(ctx context.Context, key string, data gather.Bytes) {
	if c == nil {
		return
	}

	atomic.StoreInt32(&c.anyChange, 1)

	var protected gather.WriteBuffer
	defer protected.Close()

	c.storageProtection.Protect(key, data, &protected)

	if err := c.cacheStorage.PutBlob(ctx, blob.ID(key), protected.Bytes()); err != nil {
		stats.Record(ctx, MetricStoreErrors.M(1))

		log(ctx).Errorf("unable to add %v to %v: %v", key, c.description, err)
	}
}

// Close closes the instance of persistent cache possibly waiting for at least one sweep to complete.
func (c *PersistentCache) Close(ctx context.Context) {
	if c == nil {
		return
	}

	close(c.periodicSweepClosed)
	c.periodicSweepRunning.Wait()

	// if we added anything to the cache in this sesion, run one last sweep before shutting down.
	if atomic.LoadInt32(&c.anyChange) == 1 {
		if err := c.sweepDirectory(ctx); err != nil {
			log(ctx).Errorf("error during final sweep of the %v: %v", c.description, err)
		}
	}
}

func (c *PersistentCache) sweepDirectoryPeriodically(ctx context.Context) {
	defer c.periodicSweepRunning.Done()

	for {
		select {
		case <-c.periodicSweepClosed:
			return

		case <-time.After(c.sweepFrequency):
			if err := c.sweepDirectory(ctx); err != nil {
				log(ctx).Errorf("error during periodic sweep of %v: %v", c.description, err)
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

func (c *PersistentCache) sweepDirectory(ctx context.Context) (err error) {
	timer := timetrack.StartTimer()

	var h contentMetadataHeap

	var totalRetainedSize int64

	err = c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		heap.Push(&h, it)
		totalRetainedSize += it.Length

		if totalRetainedSize > c.maxSizeBytes {
			oldest := heap.Pop(&h).(blob.Metadata) //nolint:forcetypeassert
			if delerr := c.cacheStorage.DeleteBlob(ctx, oldest.BlobID); delerr != nil {
				log(ctx).Errorf("unable to remove %v: %v", oldest.BlobID, delerr)
			} else {
				totalRetainedSize -= oldest.Length
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "error listing %v", c.description)
	}

	dur := timer.Elapsed()

	log(ctx).Debugf("finished sweeping %v in %v and retained %v/%v bytes (%v %%)", c.description, dur, totalRetainedSize, c.maxSizeBytes, 100*totalRetainedSize/c.maxSizeBytes)

	return nil
}

// NewPersistentCache creates the persistent cache in the provided storage.
func NewPersistentCache(ctx context.Context, description string, cacheStorage Storage, storageProtection StorageProtection, maxSizeBytes int64, touchThreshold, sweepFrequency time.Duration) (*PersistentCache, error) {
	if storageProtection == nil {
		storageProtection = nullStorageProtection{}
	}

	c := &PersistentCache{
		cacheStorage:        cacheStorage,
		maxSizeBytes:        maxSizeBytes,
		periodicSweepClosed: make(chan struct{}),
		touchThreshold:      touchThreshold,
		sweepFrequency:      sweepFrequency,
		description:         description,
		storageProtection:   storageProtection,
	}

	// errGood is a marker error to stop blob iteration quickly, does not
	// indicate any problem.
	errGood := errors.Errorf("good")

	// verify that cache storage is functional by listing from it
	if err := c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		return errGood
	}); err != nil && !errors.Is(err, errGood) {
		return nil, errors.Wrapf(err, "unable to open %v", c.description)
	}

	c.periodicSweepRunning.Add(1)

	go c.sweepDirectoryPeriodically(ctx)

	return c, nil
}
