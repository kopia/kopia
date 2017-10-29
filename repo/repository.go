package repo

import (
	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/metadata"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/storage"
)

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	Blocks     *block.Manager
	Objects    *object.Manager
	Metadata   *metadata.Manager
	Storage    storage.Storage
	KeyManager *auth.KeyManager
	Security   auth.SecurityOptions

	ConfigFile     string
	CacheDirectory string
}

// Close closes the repository and releases all resources.
func (r *Repository) Close() error {
	if err := r.Objects.Close(); err != nil {
		return err
	}
	if err := r.Storage.Close(); err != nil {
		return err
	}
	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush() error {
	return r.Objects.Flush()
}
