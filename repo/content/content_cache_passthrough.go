package content

import (
	"context"

	"github.com/kopia/kopia/repo/blob"
)

// passthroughContentCache is a contentCache which does no caching.
type passthroughContentCache struct {
	st blob.Storage
}

func (c passthroughContentCache) close(ctx context.Context) {}

func (c passthroughContentCache) getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64) ([]byte, error) {
	return c.st.GetBlob(ctx, blobID, offset, length)
}
