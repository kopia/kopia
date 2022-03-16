package cache

import (
	"context"

	"github.com/pkg/errors"
	"go.opencensus.io/stats"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

type contentCacheForData struct {
	pc *PersistentCache
	st blob.Storage
}

// ContentIDCacheKey computes the cache key for the provided content ID.
func ContentIDCacheKey(contentID string) string {
	// content IDs with odd length have a single-byte prefix.
	// move the prefix to the end of cache key to make sure the top level shard is spread 256 ways.
	if len(contentID)%2 == 1 {
		return contentID[1:] + contentID[0:1]
	}

	return contentID
}

// BlobIDCacheKey computes the cache key for the provided blob ID.
func BlobIDCacheKey(id blob.ID) string {
	return string(id[1:] + id[0:1])
}

func (c *contentCacheForData) GetContent(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	if c.pc.GetPartial(ctx, BlobIDCacheKey(blobID), offset, length, output) {
		return nil
	}

	output.Reset()

	// nolint:wrapcheck
	return c.pc.GetOrLoad(ctx, ContentIDCacheKey(contentID), func(output *gather.WriteBuffer) error {
		// nolint:wrapcheck
		return c.st.GetBlob(ctx, blobID, offset, length, output)
	}, output)
}

func (c *contentCacheForData) Close(ctx context.Context) {
	c.pc.Close(ctx)
}

func (c contentCacheForData) PrefetchBlob(ctx context.Context, blobID blob.ID) error {
	var blobData gather.WriteBuffer
	defer blobData.Close()

	if c.pc.GetPartial(ctx, BlobIDCacheKey(blobID), 0, 1, &blobData) {
		return nil
	}

	// read the entire blob
	if err := c.st.GetBlob(ctx, blobID, 0, -1, &blobData); err != nil {
		stats.Record(ctx, MetricMissErrors.M(1))
		// nolint:wrapcheck
		return err
	}

	stats.Record(ctx, MetricMissBytes.M(int64(blobData.Length())))

	// store the whole blob in the
	c.pc.Put(ctx, BlobIDCacheKey(blobID), blobData.Bytes())

	return nil
}

func (c contentCacheForData) CacheStorage() Storage {
	return c.pc.cacheStorage
}

// NewContentCacheForData creates new content cache for data contents.
func NewContentCacheForData(ctx context.Context, st blob.Storage, cacheStorage Storage, sweep SweepSettings, hmacSecret []byte) (ContentCache, error) {
	if cacheStorage == nil {
		return passthroughContentCache{st}, nil
	}

	pc, err := NewPersistentCache(ctx, "content cache", cacheStorage, ChecksumProtection(hmacSecret), sweep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create base cache")
	}

	return &contentCacheForData{
		st: st,
		pc: pc,
	}, nil
}
