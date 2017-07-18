package repo

import (
	"github.com/kopia/kopia/blob"
)

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	*casManager
	Storage blob.Storage
}

// Close closes the repository and releases all resources.
func (r *Repository) Close() error {
	if err := r.casManager.Close(); err != nil {
		return err
	}
	if err := r.Storage.Close(); err != nil {
		return err
	}
	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush() error {
	r.casManager.writeBack.flush()
	return nil
}

// NewRepository initializes a new repository with a given format.
func NewRepository(s blob.Storage, f *Format, options ...RepositoryOption) (*Repository, error) {
	cm, err := newCASManager(s, f, options...)
	if err != nil {
		return nil, err
	}
	return &Repository{
		Storage:    s,
		casManager: cm,
	}, nil
}
