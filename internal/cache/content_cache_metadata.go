package cache

import (
	"context"
	"hash/fnv"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/impossible"
	"github.com/kopia/kopia/repo/blob"
)

const (
	metadataCacheMutexShards = 256
)

type contentCacheForMetadata struct {
	pc *PersistentCache

	st             blob.Storage
	shardedMutexes [metadataCacheMutexShards]sync.Mutex
}

func (c *contentCacheForMetadata) mutexForBlob(blobID blob.ID) *sync.Mutex {
	// hash the blob ID to pick one of the sharded mutexes.
	h := fnv.New32()
	io.WriteString(h, string(blobID)) //nolint:errcheck
	mutexID := h.Sum32() % metadataCacheMutexShards

	return &c.shardedMutexes[mutexID]
}

func (c *contentCacheForMetadata) GetContent(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	// try getting from cache first
	if c.pc.GetPartial(ctx, string(blobID), offset, length, output) {
		return nil
	}

	// lock the mutex
	m := c.mutexForBlob(blobID)
	m.Lock()
	defer m.Unlock()

	// check again to see if we perhaps lost the race and the data is now in cache.
	if c.pc.GetPartial(ctx, string(blobID), offset, length, output) {
		return nil
	}

	var blobData gather.WriteBuffer
	defer blobData.Close()

	if err := c.fetchBlobInternal(ctx, blobID, &blobData); err != nil {
		return err
	}

	if offset == 0 && length == -1 {
		_, err := blobData.Bytes().WriteTo(output)

		return errors.Wrap(err, "error copying results")
	}

	if offset < 0 || offset+length > int64(blobData.Length()) {
		return errors.Errorf("invalid (offset=%v,length=%v) for blob %q of size %v", offset, length, blobID, blobData.Length())
	}

	output.Reset()

	impossible.PanicOnError(blobData.AppendSectionTo(output, int(offset), int(length)))

	return nil
}

func (c *contentCacheForMetadata) PrefetchBlob(ctx context.Context, blobID blob.ID) error {
	var blobData gather.WriteBuffer
	defer blobData.Close()

	// lock the mutex
	m := c.mutexForBlob(blobID)
	m.Lock()
	defer m.Unlock()

	// check to see if the data is now in cache.
	if c.pc.GetPartial(ctx, string(blobID), 0, 1, &blobData) {
		return nil
	}

	return c.fetchBlobInternal(ctx, blobID, &blobData)
}

func (c *contentCacheForMetadata) fetchBlobInternal(ctx context.Context, blobID blob.ID, blobData *gather.WriteBuffer) error {
	// read the entire blob
	if err := c.st.GetBlob(ctx, blobID, 0, -1, blobData); err != nil {
		reportMissError()

		// nolint:wrapcheck
		return err
	}

	reportMissBytes(int64(blobData.Length()))

	// store the whole blob in the cache.
	c.pc.Put(ctx, string(blobID), blobData.Bytes())

	return nil
}

func (c *contentCacheForMetadata) Close(ctx context.Context) {
	c.pc.Close(ctx)
}

func (c *contentCacheForMetadata) CacheStorage() Storage {
	return c.pc.cacheStorage
}

// NewContentCacheForMetadata creates new content cache for metadata contents.
func NewContentCacheForMetadata(ctx context.Context, st blob.Storage, cacheStorage Storage, sweep SweepSettings) (ContentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	pc, err := NewPersistentCache(ctx, "metadata cache", cacheStorage, NoProtection(), sweep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForMetadata{
		st: st,
		pc: pc,
	}, nil
}
