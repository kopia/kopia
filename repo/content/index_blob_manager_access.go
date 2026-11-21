package content

import "github.com/kopia/kopia/repo/content/indexblob"

// IndexBlobManagerV1 returns the epoch-based index blob manager.
func (sm *SharedManager) IndexBlobManagerV1() *indexblob.ManagerV1 {
	return sm.indexBlobManagerV1
}
