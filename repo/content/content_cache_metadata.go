package content

import (
	"context"
	"hash/fnv"
	"io"
	"sync"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/impossible"
	"github.com/kopia/kopia/repo/blob"
)

const (
	metadataCacheSyncParallelism = 16
	metadataCacheMutexShards     = 256
)

type contentCacheForMetadata struct {
	pc *cache.PersistentCache

	st             blob.Storage
	shardedMutexes [metadataCacheMutexShards]sync.Mutex
}

// sync synchronizes metadata cache with all blobs found in the storage.
func (c *contentCacheForMetadata) sync(ctx context.Context) error {
	sem := make(chan struct{}, metadataCacheSyncParallelism)

	var eg errgroup.Group

	// list all blobs and fetch contents into cache in parallel.
	if err := c.st.ListBlobs(ctx, PackBlobIDPrefixSpecial, func(bm blob.Metadata) error {
		// acquire semaphore
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() {
				<-sem
			}()

			var tmp gather.WriteBuffer
			defer tmp.Close()

			return c.getContent(ctx, "dummy", bm.BlobID, 0, 1, &tmp)
		})

		return nil
	}); err != nil {
		return errors.Wrap(err, "error listing blobs")
	}

	return errors.Wrap(eg.Wait(), "error synchronizing metadata cache")
}

func (c *contentCacheForMetadata) mutexForBlob(blobID blob.ID) *sync.Mutex {
	// hash the blob ID to pick one of the sharded mutexes.
	h := fnv.New32()
	io.WriteString(h, string(blobID)) //nolint:errcheck
	mutexID := h.Sum32() % metadataCacheMutexShards

	return &c.shardedMutexes[mutexID]
}

func (c *contentCacheForMetadata) getContent(ctx context.Context, contentID ID, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
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

func (c *contentCacheForMetadata) prefetchBlob(ctx context.Context, blobID blob.ID) error {
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
		stats.Record(ctx, cache.MetricMissErrors.M(1))

		// nolint:wrapcheck
		return err
	}

	stats.Record(ctx, cache.MetricMissBytes.M(int64(blobData.Length())))

	// store the whole blob in the cache.
	c.pc.Put(ctx, string(blobID), blobData.Bytes())

	return nil
}

func (c *contentCacheForMetadata) close(ctx context.Context) {
	c.pc.Close(ctx)
}

func newContentCacheForMetadata(ctx context.Context, st blob.Storage, cacheStorage cache.Storage, sweep cache.SweepSettings) (contentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	pc, err := cache.NewPersistentCache(ctx, "metadata cache", cacheStorage, cache.NoProtection(), sweep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForMetadata{
		st: st,
		pc: pc,
	}, nil
}
