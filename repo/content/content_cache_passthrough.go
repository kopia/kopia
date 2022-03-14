package content

import (
	"context"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// passthroughContentCache is a contentCache which does no caching.
type passthroughContentCache struct {
	st blob.Storage
}

func (c passthroughContentCache) close(ctx context.Context) {}

func (c passthroughContentCache) getContent(ctx context.Context, contentID ID, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	// nolint:wrapcheck
	return c.st.GetBlob(ctx, blobID, offset, length, output)
}

func (c passthroughContentCache) prefetchBlob(ctx context.Context, blobID blob.ID) error {
	return nil
}
