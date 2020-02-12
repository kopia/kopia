package repo

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
)

// Repository represents storage where both content-addressable and user-addressable data is kept.
type Repository struct {
	Blobs     blob.Storage
	Content   *content.Manager
	Objects   *object.Manager
	Manifests *manifest.Manager
	UniqueID  []byte

	ConfigFile string

	formatBlob *formatBlob
	masterKey  []byte
}

// Close closes the repository and releases all resources.
func (r *Repository) Close(ctx context.Context) error {
	if err := r.Flush(ctx); err != nil {
		return errors.Wrap(err, "error flushing")
	}

	if err := r.Content.Close(ctx); err != nil {
		return errors.Wrap(err, "error closing content-addressable storage manager")
	}

	if err := r.Blobs.Close(ctx); err != nil {
		return errors.Wrap(err, "error closing blob storage")
	}

	return nil
}

// Flush waits for all in-flight writes to complete.
func (r *Repository) Flush(ctx context.Context) error {
	if err := r.Manifests.Flush(ctx); err != nil {
		return err
	}

	return r.Content.Flush(ctx)
}

// Refresh periodically makes external changes visible to repository.
func (r *Repository) Refresh(ctx context.Context) error {
	updated, err := r.Content.Refresh(ctx)
	if err != nil {
		return errors.Wrap(err, "error refreshing content index")
	}

	if !updated {
		return nil
	}

	log.Debugf("content index refreshed")

	if err := r.Manifests.Refresh(ctx); err != nil {
		return errors.Wrap(err, "error reloading manifests")
	}

	log.Debugf("manifests refreshed")

	return nil
}

// RefreshPeriodically periodically refreshes the repository to reflect the changes made by other hosts.
func (r *Repository) RefreshPeriodically(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-time.After(interval):
			if err := r.Refresh(ctx); err != nil {
				log.Warningf("error refreshing repository: %v", err)
			}
		}
	}
}
