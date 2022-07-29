package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/repo/content"
)

func (r *directRepository) RequiredFeatures() ([]feature.Required, error) {
	repoConfig, err := r.formatBlob.decryptFormatBytes(r.formatEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decrypt repository config")
	}

	return repoConfig.RequiredFeatures, nil
}

// SetParameters changes mutable repository parameters.
func (r *directRepository) SetParameters(
	ctx context.Context,
	m content.MutableParameters,
	blobcfg content.BlobCfgBlob,
	requiredFeatures []feature.Required,
) error {
	f := r.formatBlob

	repoConfig, err := f.decryptFormatBytes(r.formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	if err := m.Validate(); err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	if err := blobcfg.Validate(); err != nil {
		return errors.Wrap(err, "invalid blob-config options")
	}

	repoConfig.FormattingOptions.MutableParameters = m
	repoConfig.RequiredFeatures = requiredFeatures

	if err := encryptFormatBytes(f, repoConfig, r.formatEncryptionKey, f.UniqueID); err != nil {
		return errors.Errorf("unable to encrypt format bytes")
	}

	if err := writeBlobCfgBlob(ctx, r.blobs, f, blobcfg, r.formatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := writeFormatBlob(ctx, r.blobs, f, r.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	if cd := r.cachingOptions.CacheDirectory; cd != "" {
		if err := os.Remove(filepath.Join(cd, FormatBlobID)); err != nil {
			log(ctx).Errorf("unable to remove %s: %v", FormatBlobID, err)
		}

		if err := os.Remove(filepath.Join(cd, BlobCfgBlobID)); err != nil && !os.IsNotExist(err) {
			log(ctx).Errorf("unable to remove %s: %v", BlobCfgBlobID, err)
		}
	}

	return nil
}
