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

func (c passthroughContentCache) Close(_ context.Context) {}

func (c passthroughContentCache) GetContent(ctx context.Context, _ string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	//nolint:wrapcheck
	return c.st.GetBlob(ctx, blobID, offset, length, output)
}

func (c passthroughContentCache) PrefetchBlob(_ context.Context, _ blob.ID) error {
	return nil
}

func (c passthroughContentCache) Sync(_ context.Context, _ blob.ID) error {
	return nil
}

func (c passthroughContentCache) CacheStorage() Storage {
	return nil
}
