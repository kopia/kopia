package repo

import (
	"github.com/kopia/kopia/blob"
)

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	*ObjectManager
	*MetadataManager
	Storage blob.Storage

	ConfigFile     string
	CacheDirectory string
}

// Close closes the repository and releases all resources.
func (r *Repository) Close() error {
	if err := r.ObjectManager.Close(); err != nil {
		return err
	}
	if err := r.Storage.Close(); err != nil {
		return err
	}
	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush() error {
	r.ObjectManager.writeBack.flush()
	return nil
}

// Stats returns repository-wide statistics.
func (r *Repository) Stats() Stats {
	return r.ObjectManager.stats
}

// ResetStats resets all repository-wide statistics to zero values.
func (r *Repository) ResetStats() {
	r.ObjectManager.stats = Stats{}
}
