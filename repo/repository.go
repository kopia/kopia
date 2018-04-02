package repo

import (
	"context"

	"github.com/kopia/kopia/auth"
	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/manifest"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/storage"
)

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	Blocks     *block.Manager
	Objects    *object.Manager
	Storage    storage.Storage
	KeyManager *auth.KeyManager
	Security   auth.SecurityOptions
	Manifests  *manifest.Manager

	ConfigFile     string
	CacheDirectory string
}

// Close closes the repository and releases all resources.
func (r *Repository) Close(ctx context.Context) error {
	if err := r.Manifests.Flush(ctx); err != nil {
		return err
	}
	if err := r.Objects.Close(ctx); err != nil {
		return err
	}
	if err := r.Blocks.Flush(ctx); err != nil {
		return err
	}
	if err := r.Storage.Close(ctx); err != nil {
		return err
	}
	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush(ctx context.Context) error {
	if err := r.Manifests.Flush(ctx); err != nil {
		return err
	}
	if err := r.Objects.Flush(ctx); err != nil {
		return err
	}

	return r.Blocks.Flush(ctx)
}
