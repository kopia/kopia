package content

import (
	"context"
)

// IndexBlobReader defines content read API.
type IndexBlobReader interface {
	IndexBlobs(ctx context.Context, includeInactive bool) ([]IndexBlobInfo, error)
}
