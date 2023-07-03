package repo

import (
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
)

// GetLockingStoragePrefixes Return all prefixes that may be maintained by Object Locking.
func GetLockingStoragePrefixes() []string {
	var prefixes []string
	// collect prefixes that need to be locked on put
	for _, prefix := range content.PackBlobIDPrefixes {
		prefixes = append(prefixes, string(prefix))
	}

	prefixes = append(prefixes, indexblob.V0IndexBlobPrefix, epoch.EpochManagerIndexUberPrefix, format.KopiaRepositoryBlobID,
		format.KopiaBlobCfgBlobID)

	return prefixes
}
