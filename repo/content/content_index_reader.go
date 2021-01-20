package content

import (
	"context"

	"github.com/kopia/kopia/repo/blob"
)

// IndexBlobReader defines content read API.
type IndexBlobReader interface {
	ParseIndexBlob(ctx context.Context, blobID blob.ID) ([]Info, error)
	DecryptBlob(ctx context.Context, blobID blob.ID) ([]byte, error)
	IndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error)
}
