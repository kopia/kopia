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
	"github.com/kopia/kopia/repo/blob"
)

const (
	metadataCacheSyncParallelism = 16
	metadataCacheMutexShards     = 16
)

type contentCacheForMetadata struct {
	pc *cache.PersistentCache

	st             blob.Storage
	shardedMutexes [metadataCacheMutexShards]sync.Mutex
}

// sync synchronizes metadata cache with all blobs found in the storage.
func (c *contentCacheForMetadata) sync(ctx context.Context) error {
	sem := make(chan struct{}, metadataCacheSyncParallelism)

	log(ctx).Debugf("synchronizing metadata cache...")
	defer log(ctx).Debugf("finished synchronizing metadata cache.")

	var eg errgroup.Group

	// list all blobs and fetch contents into cache in parallel.
	if err := c.st.ListBlobs(ctx, PackBlobIDPrefixSpecial, func(bm blob.Metadata) error {
		// acquire semaphore
		sem <- struct{}{}
		eg.Go(func() error {
			defer func() {
				<-sem
			}()

			_, err := c.getContent(ctx, "dummy", bm.BlobID, 0, 1)
			return err
		})

		return nil
	}); err != nil {
		return errors.Wrap(err, "error listing blobs")
	}

	return eg.Wait()
}

func (c *contentCacheForMetadata) mutexForBlob(blobID blob.ID) *sync.Mutex {
	// hash the blob ID to pick one of the sharded mutexes.
	h := fnv.New32()
	io.WriteString(h, string(blobID)) //nolint:errcheck
	mutexID := h.Sum32() % metadataCacheMutexShards

	return &c.shardedMutexes[mutexID]
}

func (c *contentCacheForMetadata) getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64) ([]byte, error) {
	m := c.mutexForBlob(blobID)
	m.Lock()
	defer m.Unlock()

	if v := c.pc.Get(ctx, string(blobID), offset, length); v != nil {
		return v, nil
	}

	// read the entire blob
	blobData, err := c.st.GetBlob(ctx, blobID, 0, -1)

	if err != nil {
		stats.Record(ctx, cache.MetricMissErrors.M(1))
	} else {
		stats.Record(ctx, cache.MetricMissBytes.M(int64(len(blobData))))
	}

	if errors.Is(err, blob.ErrBlobNotFound) {
		// not found in underlying storage
		// nolint:wrapcheck
		return nil, err
	}

	if err != nil {
		// nolint:wrapcheck
		return nil, err
	}

	// store the whole blob in the cache.
	c.pc.Put(ctx, string(blobID), blobData)

	if offset == 0 && length == -1 {
		return blobData, nil
	}

	if offset < 0 || offset+length > int64(len(blobData)) {
		return nil, errors.Errorf("invalid (offset=%v,length=%v) for blob %q of size %v", offset, length, blobID, len(blobData))
	}

	return blobData[offset : offset+length], nil
}

func (c *contentCacheForMetadata) close(ctx context.Context) {
	c.pc.Close(ctx)
}

func newContentCacheForMetadata(ctx context.Context, st blob.Storage, cacheStorage cache.Storage, maxSizeBytes int64) (contentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	pc, err := cache.NewPersistentCache(ctx, "metadata cache", cacheStorage, cache.NoProtection(), maxSizeBytes, cache.DefaultTouchThreshold, cache.DefaultSweepFrequency)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForMetadata{
		st: st,
		pc: pc,
	}, nil
}
