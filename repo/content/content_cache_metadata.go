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

func (c *contentCacheForMetadata) getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	// try getting from cache first
	if c.pc.Get(ctx, string(blobID), offset, length, output) {
		return nil
	}

	m := c.mutexForBlob(blobID)
	m.Lock()
	defer m.Unlock()

	var blobData gather.WriteBuffer
	defer blobData.Close()

	// read the entire blob
	err := c.st.GetBlob(ctx, blobID, 0, -1, &blobData)

	if err != nil {
		stats.Record(ctx, cache.MetricMissErrors.M(1))
	} else {
		stats.Record(ctx, cache.MetricMissBytes.M(int64(blobData.Length())))
	}

	if errors.Is(err, blob.ErrBlobNotFound) {
		// not found in underlying storage
		// nolint:wrapcheck
		return err
	}

	if err != nil {
		// nolint:wrapcheck
		return err
	}

	// store the whole blob in the cache.
	c.pc.Put(ctx, string(blobID), blobData.Bytes())

	if offset == 0 && length == -1 {
		_, err := blobData.Bytes().WriteTo(output)

		return errors.Wrap(err, "error copying results")
	}

	if offset < 0 || offset+length > int64(blobData.Length()) {
		return errors.Errorf("invalid (offset=%v,length=%v) for blob %q of size %v", offset, length, blobID, blobData.Length())
	}

	output.Reset()

	if err := blobData.AppendSectionTo(output, int(offset), int(length)); err != nil {
		// should never happen
		return errors.Wrap(err, "error appending to result")
	}

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
