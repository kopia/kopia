package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

// SetParameters changes mutable repository parameters.
func (r *directRepository) SetParameters(ctx context.Context, m content.MutableParameters) error {
	f := r.formatBlob

	repoConfig, err := f.decryptFormatBytes(r.formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	if err := m.Validate(); err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	repoConfig.FormattingOptions.MutableParameters = m

	if err := encryptFormatBytes(f, repoConfig, r.formatEncryptionKey, f.UniqueID); err != nil {
		return errors.Errorf("unable to encrypt format bytes")
	}

	if err := writeFormatBlob(ctx, r.blobs, f, r.retentionBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	if cd := r.cachingOptions.CacheDirectory; cd != "" {
		if err := os.Remove(filepath.Join(cd, "kopia.repository")); err != nil && !os.IsNotExist(err) {
			return errors.Errorf("unable to remove cached repository format blob: %v", err)
		}
	}

	return nil
}
