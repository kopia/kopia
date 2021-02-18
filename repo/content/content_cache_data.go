package content

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/repo/blob"
)

type contentCacheForData struct {
	pc *cache.PersistentCache
	st blob.Storage
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

	return c.pc.GetOrLoad(ctx, string(cacheKey), func() ([]byte, error) {
		return c.st.GetBlob(ctx, blobID, offset, length)
	})
}

func (c *contentCacheForData) close(ctx context.Context) {
	c.pc.Close(ctx)
}

func newContentCacheForData(ctx context.Context, st blob.Storage, cacheStorage cache.Storage, maxSizeBytes int64, hmacSecret []byte) (contentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	pc, err := cache.NewPersistentCache(ctx, "content cache", cacheStorage, cache.ChecksumProtection(hmacSecret), maxSizeBytes, cache.DefaultTouchThreshold, cache.DefaultSweepFrequency)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForData{
		st: st,
		pc: pc,
	}, nil
}
