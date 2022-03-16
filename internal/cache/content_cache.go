package cache

import (
	"context"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// ContentCache caches contents stored in pack blobs.
type ContentCache interface {
	Close(ctx context.Context)
	GetContent(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error
	PrefetchBlob(ctx context.Context, blobID blob.ID) error
	CacheStorage() Storage
}

// SyncableContentCache caches contents stored in pack blobs and supports synchronizing.
type SyncableContentCache interface {
	ContentCache

	Sync(ctx context.Context, blobPrefix blob.ID) error
}
