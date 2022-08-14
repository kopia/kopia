package format

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/feature"
	"github.com/kopia/kopia/repo/blob"
)

// SetParameters sets the mutable repository parameters.
func (m *Manager) SetParameters(
	ctx context.Context,
	mp MutableParameters,
	blobcfg BlobStorageConfiguration,
	requiredFeatures []feature.Required,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := mp.Validate(); err != nil {
		return errors.Wrap(err, "invalid parameters")
	}

	if err := blobcfg.Validate(); err != nil {
		return errors.Wrap(err, "invalid blob-config options")
	}

	m.repoConfig.ContentFormat.MutableParameters = mp
	m.repoConfig.RequiredFeatures = requiredFeatures

	if err := m.j.EncryptRepositoryConfig(m.repoConfig, m.formatEncryptionKey); err != nil {
		return errors.Errorf("unable to encrypt format bytes")
	}

	if err := m.j.WriteBlobCfgBlob(ctx, m.blobs, blobcfg, m.formatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := m.j.WriteKopiaRepositoryBlob(ctx, m.blobs, m.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	m.cache.Remove(ctx, []blob.ID{KopiaRepositoryBlobID, KopiaBlobCfgBlobID})

	return nil
}
