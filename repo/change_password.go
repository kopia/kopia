package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// ChangePassword changes the repository password and rewrites
// `kopia.repository` & `kopia.blobcfg`.
func (r *directRepository) ChangePassword(ctx context.Context, newPassword string) error {
	f := r.formatBlob

	repoConfig, err := f.decryptFormatBytes(r.formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	if !repoConfig.EnablePasswordChange {
		return errors.Errorf("password changes are not supported for repositories created using Kopia v0.8 or older")
	}

	newFormatEncryptionKey, err := f.deriveFormatEncryptionKeyFromPassword(newPassword)
	if err != nil {
		return errors.Wrap(err, "unable to derive master key")
	}

	r.formatEncryptionKey = newFormatEncryptionKey

	if err := encryptFormatBytes(f, repoConfig, newFormatEncryptionKey, f.UniqueID); err != nil {
		return errors.Wrap(err, "unable to encrypt format bytes")
	}

	if err := writeBlobCfgBlob(ctx, r.blobs, f, r.blobCfgBlob, newFormatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := writeFormatBlob(ctx, r.blobs, f, r.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	// remove cached kopia.repository blob.
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
