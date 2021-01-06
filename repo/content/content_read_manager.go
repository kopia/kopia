package content

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// CommittedReadManager is responsible for read-only access to committed data.
type CommittedReadManager struct {
	Stats             *Stats
	st                blob.Storage
	indexBlobManager  indexBlobManager
	contentCache      contentCache
	metadataCache     contentCache
	committedContents *committedContentIndex
	hasher            hashing.HashFunc
	encryptor         encryption.Encryptor
	timeNow           func() time.Time
}
