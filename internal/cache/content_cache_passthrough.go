package cache

import (
	"context"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// passthroughContentCache is a contentCache which does no caching.
type passthroughContentCache struct {
	st blob.Storage
}

func (c passthroughContentCache) Close(ctx context.Context) {}

func (c passthroughContentCache) GetContent(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	//nolint:wrapcheck
	return c.st.GetBlob(ctx, blobID, offset, length, output)
}

func (c passthroughContentCache) PrefetchBlob(ctx context.Context, blobID blob.ID) error {
	return nil
}

func (c passthroughContentCache) Sync(ctx context.Context, blobPrefix blob.ID) error {
	return nil
}

func (c passthroughContentCache) CacheStorage() Storage {
	return nil
}
