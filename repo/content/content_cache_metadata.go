package content

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

const metadataCacheSyncParallelism = 16

type contentCacheForMetadata struct {
	*cacheBase

	st blob.Storage
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

func (c *contentCacheForMetadata) getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64) ([]byte, error) {
	m := c.perItemMutex(blobID)
	m.Lock()
	defer m.Unlock()

	useCache := shouldUseContentCache(ctx)
	if useCache {
		if v, err := c.cacheBase.cacheStorage.GetBlob(ctx, blobID, offset, length); err == nil {
			// cache hit
			stats.Record(ctx,
				metricContentCacheHitCount.M(1),
				metricContentCacheHitBytes.M(int64(len(v))),
			)

			return v, nil
		}
	}

	stats.Record(ctx, metricContentCacheMissCount.M(1))

	// read the entire blob
	log(ctx).Debugf("fetching metadata blob %q", blobID)
	blobData, err := c.st.GetBlob(ctx, blobID, 0, -1)

	if err != nil {
		stats.Record(ctx, metricContentCacheMissErrors.M(1))
	} else {
		stats.Record(ctx, metricContentCacheMissBytes.M(int64(len(blobData))))
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

	if useCache {
		// store the whole blob in the cache.
		atomic.StoreInt32(&c.anyChange, 1)

		if puterr := c.cacheStorage.PutBlob(ctx, blobID, gather.FromSlice(blobData)); puterr != nil {
			stats.Record(ctx, metricContentCacheStoreErrors.M(1))
			log(ctx).Warningf("unable to write cache item %v: %v", blobID, puterr)
		}
	}

	if offset == 0 && length == -1 {
		return blobData, nil
	}

	if offset < 0 || offset+length > int64(len(blobData)) {
		return nil, errors.Errorf("invalid (offset=%v,length=%v) for blob %q of size %v", offset, length, blobID, len(blobData))
	}

	return blobData[offset : offset+length], nil
}

func newContentCacheForMetadata(ctx context.Context, st, cacheStorage blob.Storage, maxSizeBytes int64) (contentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	cb, err := newContentCacheBase(ctx, "metadata cache", cacheStorage, maxSizeBytes, defaultTouchThreshold, defaultSweepFrequency)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForMetadata{
		st:        st,
		cacheBase: cb,
	}, nil
}
