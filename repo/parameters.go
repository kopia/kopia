package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/repo/format"
)

func (r *directRepository) RequiredFeatures() ([]feature.Required, error) {
	repoConfig, err := r.formatBlob.DecryptRepositoryConfig(r.formatEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decrypt repository config")
	}

	return repoConfig.RequiredFeatures, nil
}

// SetParameters changes mutable repository parameters.
func (r *directRepository) SetParameters(
	ctx context.Context,
	m format.MutableParameters,
	blobcfg format.BlobStorageConfiguration,
	requiredFeatures []feature.Required,
) error {
	f := r.formatBlob

	repoConfig, err := f.DecryptRepositoryConfig(r.formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	if err := m.Validate(); err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	if err := blobcfg.Validate(); err != nil {
		return errors.Wrap(err, "invalid blob-config options")
	}

	repoConfig.ContentFormat.MutableParameters = m
	repoConfig.RequiredFeatures = requiredFeatures

	if err := f.EncryptRepositoryConfig(repoConfig, r.formatEncryptionKey); err != nil {
		return errors.Errorf("unable to encrypt format bytes")
	}

	if err := f.WriteBlobCfgBlob(ctx, r.blobs, blobcfg, r.formatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := f.WriteKopiaRepositoryBlob(ctx, r.blobs, r.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	if cd := r.cachingOptions.CacheDirectory; cd != "" {
		if err := os.Remove(filepath.Join(cd, format.KopiaRepositoryBlobID)); err != nil {
			log(ctx).Errorf("unable to remove %s: %v", format.KopiaRepositoryBlobID, err)
		}

		if err := os.Remove(filepath.Join(cd, format.KopiaBlobCfgBlobID)); err != nil && !os.IsNotExist(err) {
			log(ctx).Errorf("unable to remove %s: %v", format.KopiaBlobCfgBlobID, err)
		}
	}

	return nil
}
