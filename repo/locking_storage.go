package repo

import (
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo/blob"
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
