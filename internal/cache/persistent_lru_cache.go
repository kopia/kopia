// Package cache implements durable on-disk cache with LRU expiration.
package cache

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/ctxutil"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/releasable"
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

	// Size of the mutex cache LRU.
	// In case a mutex is evicted of the cache, the impact will be some redundant read,
	// which given the size should be extremely rare.
	mutexCacheSize = 10000
)

// PersistentCache provides persistent on-disk cache.
type PersistentCache struct {
	// +checkatomic
	anyChange int32

	cacheStorage      Storage
	storageProtection StorageProtection
	sweep             SweepSettings

	description string

	periodicSweepRunning sync.WaitGroup
	periodicSweepClosed  chan struct{}

	mutexCache *lru.Cache
}

// CacheStorage returns cache storage.
func (c *PersistentCache) CacheStorage() Storage {
	return c.cacheStorage
}

// GetFetchingMutex returns a RWMutex used to lock a blob or content during loading.
func (c *PersistentCache) GetFetchingMutex(key string) *sync.RWMutex {
	if v, ok := c.mutexCache.Get(key); ok {
		// nolint:forcetypeassert
		return v.(*sync.RWMutex)
	}

	newVal := &sync.RWMutex{}

	if prevVal, ok, _ := c.mutexCache.PeekOrAdd(key, newVal); ok {
		// nolint:forcetypeassert
		return prevVal.(*sync.RWMutex)
	}

	return newVal
}

// GetOrLoad is utility function gets the provided item from the cache or invokes the provided fetch function.
// The function also appends and verifies HMAC checksums using provided secret on all cached items to ensure data integrity.
func (c *PersistentCache) GetOrLoad(ctx context.Context, key string, fetch func(output *gather.WriteBuffer) error, output *gather.WriteBuffer) error {
	if c == nil {
		// special case - also works on non-initialized cache pointer.
		return fetch(output)
	}

	if c.GetFull(ctx, key, output) {
		return nil
	}

	output.Reset()

	mut := c.GetFetchingMutex(key)
	mut.Lock()
	defer mut.Unlock()

	// check again while holding the mutex
	if c.GetFull(ctx, key, output) {
		return nil
	}

	if err := fetch(output); err != nil {
		reportMissError()

		return err
	}

	reportMissBytes(int64(output.Length()))

	c.Put(ctx, key, output.Bytes())

	return nil
}

// GetFull fetches the contents of a full blob. Returns false if not found.
func (c *PersistentCache) GetFull(ctx context.Context, key string, output *gather.WriteBuffer) bool {
	return c.GetPartial(ctx, key, 0, -1, output)
}

// GetPartial fetches the contents of a cached blob when (length < 0) or a subset of it (when length >= 0).
// returns false if not found.
func (c *PersistentCache) GetPartial(ctx context.Context, key string, offset, length int64, output *gather.WriteBuffer) bool {
	if c == nil {
		return false
	}

	var tmp gather.WriteBuffer
	defer tmp.Close()

	if err := c.cacheStorage.GetBlob(ctx, blob.ID(key), offset, length, &tmp); err == nil {
		prot := c.storageProtection
		if length >= 0 {
			// only full items have protection.
			prot = nullStorageProtection{}
		}

		if err := prot.Verify(key, tmp.Bytes(), output); err == nil {
			// cache hit
			reportHitBytes(int64(output.Length()))

			// cache hit
			c.cacheStorage.TouchBlob(ctx, blob.ID(key), c.sweep.TouchThreshold) //nolint:errcheck

			return true
		}

		// delete invalid blob
		reportMalformedData()

		if err := c.cacheStorage.DeleteBlob(ctx, blob.ID(key)); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			log(ctx).Errorf("unable to delete %v entry %v: %v", c.description, key, err)
		}
	}

	// cache miss
	l := length
	if l < 0 {
		l = 0
	}

	reportMissBytes(l)

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

	if err := c.cacheStorage.PutBlob(ctx, blob.ID(key), protected.Bytes(), blob.PutOptions{}); err != nil {
		reportStoreError()

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

	releasable.Released("persistent-cache", c)
}

func (c *PersistentCache) sweepDirectoryPeriodically(ctx context.Context) {
	defer c.periodicSweepRunning.Done()

	for {
		select {
		case <-c.periodicSweepClosed:
			return

		case <-time.After(c.sweep.SweepFrequency):
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
	*h = append(*h, x.(blob.Metadata)) // nolint:forcetypeassert
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

	var (
		totalRetainedSize int64
		tooRecentBytes    int64
		tooRecentCount    int
	)

	err = c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		// ignore items below minimal age.
		if age := clock.Now().Sub(it.Timestamp); age < c.sweep.MinSweepAge {
			tooRecentCount++
			tooRecentBytes += it.Length

			return nil
		}

		heap.Push(&h, it)
		totalRetainedSize += it.Length

		if totalRetainedSize > c.sweep.MaxSizeBytes {
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

	const hundredPercent = 100

	inUsePercent := int64(hundredPercent)

	if c.sweep.MaxSizeBytes != 0 {
		inUsePercent = hundredPercent * totalRetainedSize / c.sweep.MaxSizeBytes
	}

	log(ctx).Debugw(
		"finished sweeping",
		"cache", c.description,
		"duration", dur,
		"totalRetainedSize", totalRetainedSize,
		"tooRecentBytes", tooRecentBytes,
		"tooRecentCount", tooRecentCount,
		"maxSizeBytes", c.sweep.MaxSizeBytes,
		"inUsePercent", inUsePercent,
	)

	return nil
}

// SweepSettings encapsulates settings that impact cache item sweep/expiration.
type SweepSettings struct {
	MaxSizeBytes   int64
	SweepFrequency time.Duration
	MinSweepAge    time.Duration
	TouchThreshold time.Duration
}

func (s SweepSettings) applyDefaults() SweepSettings {
	if s.TouchThreshold == 0 {
		s.TouchThreshold = DefaultTouchThreshold
	}

	if s.SweepFrequency == 0 {
		s.SweepFrequency = DefaultSweepFrequency
	}

	return s
}

// NewPersistentCache creates the persistent cache in the provided storage.
func NewPersistentCache(ctx context.Context, description string, cacheStorage Storage, storageProtection StorageProtection, sweep SweepSettings) (*PersistentCache, error) {
	if cacheStorage == nil {
		return nil, nil
	}

	sweep = sweep.applyDefaults()

	if storageProtection == nil {
		storageProtection = NoProtection()
	}

	c := &PersistentCache{
		cacheStorage:        cacheStorage,
		sweep:               sweep,
		periodicSweepClosed: make(chan struct{}),
		description:         description,
		storageProtection:   storageProtection,
	}

	c.mutexCache, _ = lru.New(mutexCacheSize)

	// verify that cache storage is functional by listing from it
	if _, err := c.cacheStorage.GetMetadata(ctx, "test-blob"); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
		return nil, errors.Wrapf(err, "unable to open %v", c.description)
	}

	releasable.Created("persistent-cache", c)

	c.periodicSweepRunning.Add(1)

	go c.sweepDirectoryPeriodically(ctxutil.Detach(ctx))

	return c, nil
}
