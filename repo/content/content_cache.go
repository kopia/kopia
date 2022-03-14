package content

import (
	"context"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

type contentCache interface {
	close(ctx context.Context)
	getContent(ctx context.Context, contentID ID, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error
	prefetchBlob(ctx context.Context, blobID blob.ID) error
}
