package repo

import (
	"context"
	"strings"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/beforeop"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
)

// GetLockingStoragePrefixes Return all prefixes that may be maintained by Object Locking.
func GetLockingStoragePrefixes() []blob.ID {
	return append([]blob.ID{
		blob.ID(indexblob.V0IndexBlobPrefix),
		blob.ID(epoch.EpochManagerIndexUberPrefix),
		blob.ID(format.KopiaRepositoryBlobID),
		blob.ID(format.KopiaBlobCfgBlobID),
	}, content.PackBlobIDPrefixes...)
}

// IsLockingStorageBlobID returns true when the given blob ID matches one of the
// prefixes that may be maintained by Object Locking.
func IsLockingStorageBlobID(id blob.ID) bool {
	for _, prefix := range GetLockingStoragePrefixes() {
		if strings.HasPrefix(string(id), string(prefix)) {
			return true
		}
	}

	return false
}

// WrapLockingStorage wraps the given storage so that PutBlob for blobs matching
// GetLockingStoragePrefixes carries the retention options from the given
// blob storage configuration.
func WrapLockingStorage(st blob.Storage, r format.BlobStorageConfiguration) blob.Storage {
	return beforeop.NewWrapper(st, nil, nil, nil, func(_ context.Context, id blob.ID, opts *blob.PutOptions) error {
		if IsLockingStorageBlobID(id) {
			opts.RetentionMode = r.RetentionMode
			opts.RetentionPeriod = r.RetentionPeriod
		}

		return nil
	})
}
