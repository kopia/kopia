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
	listCacheMutex sync.Mutex
	// +checklocks:listCacheMutex
	listCache contentMetadataHeap

	cacheStorage      Storage
	storageProtection cacheprot.StorageProtection
	sweep             SweepSettings
	timeNow           func() time.Time
	lastCacheWarning  time.Time

	description string

	metricsStruct
}

// CacheStorage returns cache storage.
func (c *PersistentCache) CacheStorage() Storage {
	return c.cacheStorage
}

// GetFetchingMutex returns a RWMutex used to lock a blob or content during loading.
func (c *PersistentCache) GetFetchingMutex(id blob.ID) *sync.RWMutex {
	if c == nil {
		// special case - also works on non-initialized cache pointer.
		return &sync.RWMutex{}
	}

	c.listCacheMutex.Lock()
	defer c.listCacheMutex.Unlock()

	if _, entry := c.listCache.LookupByID(id); entry != nil {
		return &entry.contentDownloadMutex
	}

	heap.Push(&c.listCache, blob.Metadata{BlobID: id})

	_, entry := c.listCache.LookupByID(id)

	return &entry.contentDownloadMutex
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

	mut := c.GetFetchingMutex(blob.ID(key))
	mut.Lock()
	defer mut.Unlock()

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

	// cache hit
	c.listCacheMutex.Lock()
	defer c.listCacheMutex.Unlock()

	// Touching the blobs when cache is full can lead to cache never
	// getting cleaned up if all the blobs fall under MinSweepAge.
	//
	// This can happen when the user is restoring large files (at
	// comparable sizes to the cache size limitation) and MinSweepAge is
	// sufficiently large. For large files which span over multiple
	// blobs, every blob becomes least-recently-used.
	//
	// So, we'll avoid this until our cache usage drops to acceptable
	// limits.
	if c.isCacheFullLocked() {
		c.listCacheCleanupLocked(ctx)

		if c.isCacheFullLocked() {
			return
		}
	}

	// unlock for the expensive operation
	c.listCacheMutex.Unlock()
	mtime, err := c.cacheStorage.TouchBlob(ctx, blob.ID(key), c.sweep.TouchThreshold)
	c.listCacheMutex.Lock()

	if err == nil {
		// insert or update the metadata
		heap.Push(&c.listCache, blob.Metadata{
			BlobID:    blob.ID(key),
			Length:    length,
			Timestamp: mtime,
		})
	}
}

func (c *PersistentCache) getPartialDeleteInvalidBlob(ctx context.Context, key string) {
	// delete invalid blob
	c.reportMalformedData()

	if err := c.cacheStorage.DeleteBlob(ctx, blob.ID(key)); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
		log(ctx).Errorf("unable to delete %v entry %v: %v", c.description, key, err)
	} else {
		c.listCacheMutex.Lock()
		if i, entry := c.listCache.LookupByID(blob.ID(key)); entry != nil {
			heap.Remove(&c.listCache, i)
		}
		c.listCacheMutex.Unlock()
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
		prot := c.storageProtection
		if length >= 0 {
			// only full items have protection.
			prot = cacheprot.NoProtection()
		}

		if err := prot.Verify(key, tmp.Bytes(), output); err == nil {
			c.getPartialCacheHit(ctx, key, length, output)

			return true
		}

		c.getPartialDeleteInvalidBlob(ctx, key)
	}

	// cache miss
	l := length
	if l < 0 {
		l = 0
	}

	c.reportMissBytes(l)

	return false
}

// +checklocks:c.listCacheMutex
func (c *PersistentCache) isCacheFullLocked() bool {
	return c.listCache.DataSize() > c.sweep.MaxSizeBytes
}

// Put adds the provided key-value pair to the cache.
func (c *PersistentCache) Put(ctx context.Context, key string, data gather.Bytes) {
	if c == nil {
		return
	}

	var (
		protected gather.WriteBuffer
		mtime     time.Time
	)

	defer protected.Close()

	c.listCacheMutex.Lock()
	defer c.listCacheMutex.Unlock()

	// opportunistically cleanup cache before the PUT if we can
	if c.isCacheFullLocked() {
		c.listCacheCleanupLocked(ctx)
		// Do not add more things to cache if it remains full after cleanup. We
		// MUST NOT go over the specified limit for the cache space to avoid
		// snapshots/restores from getting affected by the cache's storage use.
		if c.isCacheFullLocked() {
			// Limit warnings to one per minute max.
			if clock.Now().Sub(c.lastCacheWarning) > 10*time.Minute {
				c.lastCacheWarning = clock.Now()

				log(ctx).Warnf("Cache is full, unable to add item into '%s' cache.", c.description)
			}

			return
		}
	}

	// LOCK RELEASED for expensive operations
	c.listCacheMutex.Unlock()
	c.storageProtection.Protect(key, data, &protected)

	if err := c.cacheStorage.PutBlob(ctx, blob.ID(key), protected.Bytes(), blob.PutOptions{GetModTime: &mtime}); err != nil {
		c.reportStoreError()

		log(ctx).Errorf("unable to add %v to %v: %v", key, c.description, err)
	}

	c.listCacheMutex.Lock()
	// LOCK RE-ACQUIRED

	c.listCache.Push(blob.Metadata{
		BlobID:    blob.ID(key),
		Length:    int64(protected.Bytes().Length()),
		Timestamp: mtime,
	})

	c.listCacheCleanupLocked(ctx)
}

// Close closes the instance of persistent cache possibly waiting for at least one sweep to complete.
func (c *PersistentCache) Close(ctx context.Context) {
	if c == nil {
		return
	}

	releasable.Released("persistent-cache", c)
}

type blobCacheEntry struct {
	metadata             blob.Metadata
	contentDownloadMutex sync.RWMutex
}

// A contentMetadataHeap implements heap.Interface and holds blob.Metadata.
type contentMetadataHeap struct {
	data     []*blobCacheEntry
	index    map[blob.ID]int
	dataSize int64
}

func newContentMetadataHeap() contentMetadataHeap {
	return contentMetadataHeap{index: make(map[blob.ID]int)}
}

func (h contentMetadataHeap) Len() int { return len(h.data) }

func (h contentMetadataHeap) Less(i, j int) bool {
	return h.data[i].metadata.Timestamp.Before(h.data[j].metadata.Timestamp)
}

func (h contentMetadataHeap) Swap(i, j int) {
	h.index[h.data[i].metadata.BlobID], h.index[h.data[j].metadata.BlobID] = h.index[h.data[j].metadata.BlobID], h.index[h.data[i].metadata.BlobID]
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *contentMetadataHeap) Push(x interface{}) {
	bm := x.(blob.Metadata) //nolint:forcetypeassert
	if i, exists := h.index[bm.BlobID]; exists {
		// only accept newer timestamps
		if h.data[i].metadata.Timestamp.IsZero() || bm.Timestamp.After(h.data[i].metadata.Timestamp) {
			h.dataSize += bm.Length - h.data[i].metadata.Length
			h.data[i] = &blobCacheEntry{metadata: bm}
			heap.Fix(h, i)
		}
	} else {
		h.index[bm.BlobID] = len(h.data)
		h.data = append(h.data, &blobCacheEntry{metadata: bm})
		h.dataSize += bm.Length
	}
}

func (h *contentMetadataHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	item := old[n-1]
	h.data = old[0 : n-1]
	h.dataSize -= item.metadata.Length
	delete(h.index, item.metadata.BlobID)

	return item.metadata
}

func (h *contentMetadataHeap) LookupByID(id blob.ID) (int, *blobCacheEntry) {
	i, ok := h.index[id]
	if !ok {
		return -1, nil
	}

	return i, h.data[i]
}

func (h contentMetadataHeap) DataSize() int64 { return h.dataSize }

// +checklocks:c.listCacheMutex
func (c *PersistentCache) listCacheCleanupLocked(ctx context.Context) {
	var (
		unsuccessfulDeletes     []blob.Metadata
		unsuccessfulDeletesSize int64
		now                     = c.timeNow()
	)

	// if there are blobs pending to be deleted ...
	for c.listCache.DataSize() > 0 &&
		// ... and everything including what we couldn't delete is still bigger than the threshold
		(c.listCache.DataSize()+unsuccessfulDeletesSize) > c.sweep.MaxSizeBytes {
		oldest := heap.Pop(&c.listCache).(blob.Metadata) //nolint:forcetypeassert

		// stop here if the oldest item is below the specified minimal age
		if age := now.Sub(oldest.Timestamp); age < c.sweep.MinSweepAge {
			heap.Push(&c.listCache, oldest)
			break
		}

		// unlock before the expensive operation
		c.listCacheMutex.Unlock()
		delerr := c.cacheStorage.DeleteBlob(ctx, oldest.BlobID)
		c.listCacheMutex.Lock()

		if delerr != nil {
			log(ctx).Errorf("unable to remove %v: %v", oldest.BlobID, delerr)
			// accumulate unsuccessful deletes to be pushed back into the heap
			// later so we do not attempt deleting the same blob multiple times
			//
			// after this we keep draining from the heap until we bring down
			// c.listCache.DataSize() to zero
			unsuccessfulDeletes = append(unsuccessfulDeletes, oldest)
			unsuccessfulDeletesSize += oldest.Length
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

	if c.isCacheFullLocked() {
		c.listCacheCleanupLocked(ctx)
	}

	dur := timer.Elapsed()

	const hundredPercent = 100

	inUsePercent := int64(hundredPercent)

	if c.sweep.MaxSizeBytes != 0 {
		inUsePercent = hundredPercent * c.listCache.DataSize() / c.sweep.MaxSizeBytes
	}

	log(ctx).Debugw(
		"finished initial cache scan",
		"cache", c.description,
		"duration", dur,
		"totalRetainedSize", c.listCache.DataSize(),
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
	MinSweepAge    time.Duration
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
