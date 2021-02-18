package content

import (
	"context"

	"github.com/kopia/kopia/repo/blob"
)

type cacheKey string

type contentCache interface {
	close(ctx context.Context)
	getContent(ctx context.Context, cacheKey cacheKey, blobID blob.ID, offset, length int64) ([]byte, error)
}
