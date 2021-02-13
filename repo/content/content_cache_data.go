package content

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/repo/blob"
)

type contentCacheForData struct {
	*cacheBase

	st         blob.Storage
	hmacSecret []byte
}

func adjustCacheKey(cacheKey cacheKey) cacheKey {
	// content IDs with odd length have a single-byte prefix.
	// move the prefix to the end of cache key to make sure the top level shard is spread 256 ways.
	if len(cacheKey)%2 == 1 {
		return cacheKey[1:] + cacheKey[0:1]
	}

	return cacheKey
}

func (c *contentCacheForData) getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64) ([]byte, error) {
	cacheKey = adjustCacheKey(cacheKey)

	useCache := shouldUseContentCache(ctx)
	if useCache {
		if b := c.readAndVerifyCacheContent(ctx, cacheKey); b != nil {
			stats.Record(ctx,
				metricContentCacheHitCount.M(1),
				metricContentCacheHitBytes.M(int64(len(b))),
			)

			return b, nil
		}
	}

	stats.Record(ctx, metricContentCacheMissCount.M(1))

	b, err := c.st.GetBlob(ctx, blobID, offset, length)
	if err != nil {
		stats.Record(ctx, metricContentCacheMissErrors.M(1))
	} else {
		stats.Record(ctx, metricContentCacheMissBytes.M(int64(len(b))))
	}

	if errors.Is(err, blob.ErrBlobNotFound) {
		// not found in underlying storage
		// nolint:wrapcheck
		return nil, err
	}

	if err == nil && useCache {
		atomic.StoreInt32(&c.anyChange, 1)

		if puterr := c.cacheStorage.PutBlob(ctx, blob.ID(cacheKey), gather.FromSlice(hmac.Append(b, c.hmacSecret))); puterr != nil {
			stats.Record(ctx, metricContentCacheStoreErrors.M(1))
			log(ctx).Warningf("unable to write cache item %v: %v", cacheKey, puterr)
		}
	}

	return b, errors.Wrap(err, "error getting content from cache")
}

func (c *contentCacheForData) readAndVerifyCacheContent(ctx context.Context, cacheKey cacheKey) []byte {
	b, err := c.cacheStorage.GetBlob(ctx, blob.ID(cacheKey), 0, -1)
	if err == nil {
		b, err = hmac.VerifyAndStrip(b, c.hmacSecret)
		if err == nil {
			c.touch(ctx, blob.ID(cacheKey))

			// retrieved from cache and HMAC valid
			return b
		}

		// ignore malformed contents
		log(ctx).Warningf("malformed content %v: %v", cacheKey, err)

		return nil
	}

	if !errors.Is(err, blob.ErrBlobNotFound) {
		log(ctx).Warningf("unable to read cache %v: %v", cacheKey, err)
	}

	return nil
}

func newContentCacheForData(ctx context.Context, st, cacheStorage blob.Storage, maxSizeBytes int64, hmacSecret []byte) (contentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	cb, err := newContentCacheBase(ctx, "content cache", cacheStorage, maxSizeBytes, defaultTouchThreshold, defaultSweepFrequency)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForData{
		st:         st,
		hmacSecret: append([]byte(nil), hmacSecret...),
		cacheBase:  cb,
	}, nil
}
