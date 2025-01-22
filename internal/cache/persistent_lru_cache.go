// Package cache implements durable on-disk cache with LRU expiration.
package cache

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cacheprot"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/releasable"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("cache")

const (
	// DefaultTouchThreshold specifies the resolution of timestamps used to determine which cache items
	// to expire. This helps cache storage writes on frequently accessed items.
	DefaultTouchThreshold = 10 * time.Minute
)

// PersistentCache provides persistent on-disk cache.
type PersistentCache struct {
	fetchMutexes mutexMap

	listCacheMutex sync.Mutex
	// +checklocks:listCacheMutex
	listCache contentMetadataHeap
	// +checklocks:listCacheMutex
	pendingWriteBytes int64

	cacheStorage      Storage
	storageProtection cacheprot.StorageProtection
	sweep             SweepSettings
	timeNow           func() time.Time

	// +checklocks:listCacheMutex
	lastCacheWarning time.Time

	description string

	metricsStruct
}

// CacheStorage returns cache storage.
func (c *PersistentCache) CacheStorage() Storage {
	return c.cacheStorage
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

	c.exclusiveLock(key)
	defer c.exclusiveUnlock(key)

	// check again while holding the mutex
	if c.GetFull(ctx, key, output) {
		return nil
	}

	if err := fetch(output); err != nil {
		c.reportMissError()

		return err
	}

	c.reportMissBytes(int64(output.Length()))

	c.Put(ctx, key, output.Bytes())

	return nil
}

// GetFull fetches the contents of a full blob. Returns false if not found.
func (c *PersistentCache) GetFull(ctx context.Context, key string, output *gather.WriteBuffer) bool {
	return c.GetPartial(ctx, key, 0, -1, output)
}

func (c *PersistentCache) getPartialCacheHit(ctx context.Context, key string, length int64, output *gather.WriteBuffer) {
	// cache hit
	c.reportHitBytes(int64(output.Length()))

	mtime, err := c.cacheStorage.TouchBlob(ctx, blob.ID(key), c.sweep.TouchThreshold)
	c.listCacheMutex.Lock()
	defer c.listCacheMutex.Unlock()

	if err == nil {
		c.listCache.AddOrUpdate(blob.Metadata{
			BlobID:    blob.ID(key),
			Length:    length,
			Timestamp: mtime,
		})
	}
}

func (c *PersistentCache) deleteInvalidBlob(ctx context.Context, key string) {
	if err := c.cacheStorage.DeleteBlob(ctx, blob.ID(key)); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
		log(ctx).Errorf("unable to delete %v entry %v: %v", c.description, key, err)
		return
	}

	c.listCacheMutex.Lock()
	defer c.listCacheMutex.Unlock()

	if i, ok := c.listCache.index[blob.ID(key)]; ok {
		heap.Remove(&c.listCache, i)
	}
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
		sp := c.storageProtection

		if length >= 0 {
			// do not perform integrity check on partial reads
			sp = cacheprot.NoProtection()
		}

		if err := sp.Verify(key, tmp.Bytes(), output); err == nil {
			c.getPartialCacheHit(ctx, key, length, output)

			return true
		}

		c.reportMalformedData()
		c.deleteInvalidBlob(ctx, key)
	}

	// cache miss
	l := length
	if l < 0 {
		l = 0
	}

	c.reportMissBytes(l)

	return false
}

// Put adds the provided key-value pair to the cache.
func (c *PersistentCache) Put(ctx context.Context, key string, data gather.Bytes) {
	if c == nil {
		return
	}

	c.listCacheMutex.Lock()
	defer c.listCacheMutex.Unlock()

	// make sure the cache has enough room for the new item including any protection overhead.
	l := data.Length() + c.storageProtection.OverheadBytes()
	c.pendingWriteBytes += int64(l)
	c.sweepLocked(ctx)

	// LOCK RELEASED for expensive operations
	c.listCacheMutex.Unlock()

	var protected gather.WriteBuffer
	defer protected.Close()

	c.storageProtection.Protect(key, data, &protected)

	if protected.Length() != l {
		log(ctx).Panicf("protection overhead mismatch, assumed %v got %v", l, protected.Length())
	}

	var mtime time.Time

	if err := c.cacheStorage.PutBlob(ctx, blob.ID(key), protected.Bytes(), blob.PutOptions{GetModTime: &mtime}); err != nil {
		c.reportStoreError()

		log(ctx).Errorf("unable to add %v to %v: %v", key, c.description, err)
	}

	c.listCacheMutex.Lock()
	// LOCK RE-ACQUIRED

	c.pendingWriteBytes -= int64(protected.Length())
	c.listCache.AddOrUpdate(blob.Metadata{
		BlobID:    blob.ID(key),
		Length:    int64(protected.Bytes().Length()),
		Timestamp: mtime,
	})
}

// Close closes the instance of persistent cache possibly waiting for at least one sweep to complete.
func (c *PersistentCache) Close(ctx context.Context) {
	if c == nil {
		return
	}

	releasable.Released("persistent-cache", c)
}

// A contentMetadataHeap implements heap.Interface and holds blob.Metadata.
//
//nolint:recvcheck
type contentMetadataHeap struct {
	data           []blob.Metadata
	index          map[blob.ID]int
	totalDataBytes int64
}

func newContentMetadataHeap() contentMetadataHeap {
	return contentMetadataHeap{index: make(map[blob.ID]int)}
}

func (h contentMetadataHeap) Len() int { return len(h.data) }

func (h contentMetadataHeap) Less(i, j int) bool {
	return h.data[i].Timestamp.Before(h.data[j].Timestamp)
}

func (h contentMetadataHeap) Swap(i, j int) {
	iBlobID := h.data[i].BlobID
	jBlobID := h.data[j].BlobID

	h.index[iBlobID], h.index[jBlobID] = h.index[jBlobID], h.index[iBlobID]
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *contentMetadataHeap) Push(x any) {
	bm := x.(blob.Metadata) //nolint:forcetypeassert

	h.index[bm.BlobID] = len(h.data)
	h.data = append(h.data, bm)
	h.totalDataBytes += bm.Length
}

func (h *contentMetadataHeap) AddOrUpdate(bm blob.Metadata) {
	if i, exists := h.index[bm.BlobID]; exists {
		// only accept newer timestamps
		if bm.Timestamp.After(h.data[i].Timestamp) {
			h.totalDataBytes += bm.Length - h.data[i].Length
			h.data[i] = bm
			heap.Fix(h, i)
		}
	} else {
		heap.Push(h, bm)
	}
}

func (h *contentMetadataHeap) Pop() any {
	old := h.data
	n := len(old)
	item := old[n-1]
	h.data = old[0 : n-1]
	h.totalDataBytes -= item.Length
	delete(h.index, item.BlobID)

	return item
}

// +checklocks:c.listCacheMutex
func (c *PersistentCache) aboveSoftLimit(extraBytes int64) bool {
	return c.listCache.totalDataBytes+extraBytes+c.pendingWriteBytes > c.sweep.MaxSizeBytes
}

// +checklocks:c.listCacheMutex
func (c *PersistentCache) aboveHardLimit(extraBytes int64) bool {
	if c.sweep.LimitBytes <= 0 {
		return false
	}

	return c.listCache.totalDataBytes+extraBytes+c.pendingWriteBytes > c.sweep.LimitBytes
}

// +checklocks:c.listCacheMutex
func (c *PersistentCache) sweepLocked(ctx context.Context) {
	var (
		unsuccessfulDeletes     []blob.Metadata
		unsuccessfulDeleteBytes int64
		now                     = c.timeNow()
	)

	for len(c.listCache.data) > 0 && (c.aboveSoftLimit(unsuccessfulDeleteBytes) || c.aboveHardLimit(unsuccessfulDeleteBytes)) {
		// examine the oldest cache item without removing it from the heap.
		oldest := c.listCache.data[0]

		if age := now.Sub(oldest.Timestamp); age < c.sweep.MinSweepAge && !c.aboveHardLimit(unsuccessfulDeleteBytes) {
			// the oldest item is below the specified minimal sweep age and we're below the hard limit, stop here
			break
		}

		heap.Pop(&c.listCache)

		if delerr := c.cacheStorage.DeleteBlob(ctx, oldest.BlobID); delerr != nil {
			log(ctx).Warnw("unable to remove cache item", "cache", c.description, "item", oldest.BlobID, "err", delerr)

			// accumulate unsuccessful deletes to be pushed back into the heap
			// later so we do not attempt deleting the same blob multiple times
			//
			// after this we keep draining from the heap until we bring down
			// c.listCache.DataSize() to zero
			unsuccessfulDeletes = append(unsuccessfulDeletes, oldest)
			unsuccessfulDeleteBytes += oldest.Length
		}
	}

	// put all unsuccessful deletes back into the heap
	for _, m := range unsuccessfulDeletes {
		heap.Push(&c.listCache, m)
	}
}

func (c *PersistentCache) initialScan(ctx context.Context) error {
	timer := timetrack.StartTimer()

	var (
		tooRecentBytes int64
		tooRecentCount int
		now            = c.timeNow()
	)

	c.listCacheMutex.Lock()
	defer c.listCacheMutex.Unlock()

	err := c.cacheStorage.ListBlobs(ctx, "", func(it blob.Metadata) error {
		// count items below minimal age.
		if age := now.Sub(it.Timestamp); age < c.sweep.MinSweepAge {
			tooRecentCount++
			tooRecentBytes += it.Length
		}

		heap.Push(&c.listCache, it) // +checklocksignore

		return nil
	})
	if err != nil {
		return errors.Wrapf(err, "error listing %v", c.description)
	}

	c.sweepLocked(ctx)

	dur := timer.Elapsed()

	const hundredPercent = 100

	inUsePercent := int64(hundredPercent)

	if c.sweep.MaxSizeBytes != 0 {
		inUsePercent = hundredPercent * c.listCache.totalDataBytes / c.sweep.MaxSizeBytes
	}

	log(ctx).Debugw(
		"finished initial cache scan",
		"cache", c.description,
		"duration", dur,
		"totalRetainedSize", c.listCache.totalDataBytes,
		"tooRecentBytes", tooRecentBytes,
		"tooRecentCount", tooRecentCount,
		"maxSizeBytes", c.sweep.MaxSizeBytes,
		"limitBytes", c.sweep.LimitBytes,
		"inUsePercent", inUsePercent,
	)

	return nil
}

func (c *PersistentCache) exclusiveLock(key string) {
	if c != nil {
		c.fetchMutexes.exclusiveLock(key)
	}
}

func (c *PersistentCache) exclusiveUnlock(key string) {
	if c != nil {
		c.fetchMutexes.exclusiveUnlock(key)
	}
}

func (c *PersistentCache) sharedLock(key string) {
	if c != nil {
		c.fetchMutexes.sharedLock(key)
	}
}

func (c *PersistentCache) sharedUnlock(key string) {
	if c != nil {
		c.fetchMutexes.sharedUnlock(key)
	}
}

// SweepSettings encapsulates settings that impact cache item sweep/expiration.
type SweepSettings struct {
	// soft limit, the cache will be limited to this size, except for items newer than MinSweepAge.
	MaxSizeBytes int64

	// hard limit, if non-zero the cache will be limited to this size, regardless of MinSweepAge.
	LimitBytes int64

	// items older than this will never be removed from the cache except when the cache is above
	// HardMaxSizeBytes.
	MinSweepAge time.Duration

	// on each use, items will be touched if they have not been touched in this long.
	TouchThreshold time.Duration
}

func (s SweepSettings) applyDefaults() SweepSettings {
	if s.TouchThreshold == 0 {
		s.TouchThreshold = DefaultTouchThreshold
	}

	return s
}

// NewPersistentCache creates the persistent cache in the provided storage.
func NewPersistentCache(ctx context.Context, description string, cacheStorage Storage, storageProtection cacheprot.StorageProtection, sweep SweepSettings, mr *metrics.Registry, timeNow func() time.Time) (*PersistentCache, error) {
	if cacheStorage == nil {
		return nil, nil
	}

	sweep = sweep.applyDefaults()

	if storageProtection == nil {
		storageProtection = cacheprot.NoProtection()
	}

	c := &PersistentCache{
		cacheStorage:      cacheStorage,
		sweep:             sweep,
		description:       description,
		storageProtection: storageProtection,
		metricsStruct:     initMetricsStruct(mr, description),
		listCache:         newContentMetadataHeap(),
		timeNow:           timeNow,
		lastCacheWarning:  time.Time{},
	}

	if c.timeNow == nil {
		c.timeNow = clock.Now
	}

	// verify that cache storage is functional by listing from it
	if _, err := c.cacheStorage.GetMetadata(ctx, "test-blob"); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
		return nil, errors.Wrapf(err, "unable to open %v", c.description)
	}

	releasable.Created("persistent-cache", c)

	if err := c.initialScan(ctx); err != nil {
		return nil, errors.Wrapf(err, "error during initial scan of %s", c.description)
	}

	return c, nil
}
