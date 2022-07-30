package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/format"
)

// ChangePassword changes the repository password and rewrites
// `kopia.repository` & `kopia.blobcfg`.
func (r *directRepository) ChangePassword(ctx context.Context, newPassword string) error {
	f := r.formatBlob

	repoConfig, err := f.DecryptRepositoryConfig(r.formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	if !repoConfig.EnablePasswordChange {
		return errors.Errorf("password changes are not supported for repositories created using Kopia v0.8 or older")
	}

	newFormatEncryptionKey, err := f.DeriveFormatEncryptionKeyFromPassword(newPassword)
	if err != nil {
		return errors.Wrap(err, "unable to derive master key")
	}

	r.formatEncryptionKey = newFormatEncryptionKey

	if err := f.EncryptRepositoryConfig(repoConfig, newFormatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to encrypt format bytes")
	}

	if err := f.WriteBlobCfgBlob(ctx, r.blobs, r.blobCfgBlob, newFormatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := f.WriteKopiaRepositoryBlob(ctx, r.blobs, r.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	// remove cached kopia.repository blob.
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
