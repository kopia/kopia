package content

import (
	"context"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

type contentCacheForData struct {
	pc *cache.PersistentCache
	st blob.Storage
}

func contentIDCacheKey(contentID ID) string {
	// content IDs with odd length have a single-byte prefix.
	// move the prefix to the end of cache key to make sure the top level shard is spread 256 ways.
	if len(contentID)%2 == 1 {
		return string(contentID[1:] + contentID[0:1])
	}

	return string(contentID)
}

func blobIDCacheKey(id blob.ID) string {
	return string(id[1:] + id[0:1])
}

func (c *contentCacheForData) getContent(ctx context.Context, contentID ID, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	if c.pc.GetPartial(ctx, blobIDCacheKey(blobID), offset, length, output) {
		return nil
	}

	output.Reset()

	// nolint:wrapcheck
	return c.pc.GetOrLoad(ctx, contentIDCacheKey(contentID), func(output *gather.WriteBuffer) error {
		// nolint:wrapcheck
		return c.st.GetBlob(ctx, blobID, offset, length, output)
	}, output)
}

func (c *contentCacheForData) close(ctx context.Context) {
	c.pc.Close(ctx)
}

func (c contentCacheForData) prefetchBlob(ctx context.Context, blobID blob.ID) error {
	var blobData gather.WriteBuffer
	defer blobData.Close()

	if c.pc.GetPartial(ctx, blobIDCacheKey(blobID), 0, 1, &blobData) {
		return nil
	}

	// read the entire blob
	if err := c.st.GetBlob(ctx, blobID, 0, -1, &blobData); err != nil {
		stats.Record(ctx, cache.MetricMissErrors.M(1))
		// nolint:wrapcheck
		return err
	}

	stats.Record(ctx, cache.MetricMissBytes.M(int64(blobData.Length())))

	// store the whole blob in the cache.
	c.pc.Put(ctx, blobIDCacheKey(blobID), blobData.Bytes())

	return nil
}

func newContentCacheForData(ctx context.Context, st blob.Storage, cacheStorage cache.Storage, sweep cache.SweepSettings, hmacSecret []byte) (contentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	pc, err := cache.NewPersistentCache(ctx, "content cache", cacheStorage, cache.ChecksumProtection(hmacSecret), sweep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForData{
		st: st,
		pc: pc,
	}, nil
}
