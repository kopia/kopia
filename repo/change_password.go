package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// ChangePassword changes the repository password and rewrites
// `kopia.repository` & `kopia.retention`.
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

	if !r.retentionBlob.IsNull() {
		retentionBytes, err := serializeRetentionBytes(f, r.retentionBlob, newFormatEncryptionKey)
		if err != nil {
			return errors.Wrap(err, "unable to encrypt retention bytes")
		}

		// TODO: no need to put a blob when nil
		if err := r.blobs.PutBlob(ctx, RetentionBlobID, gather.FromSlice(retentionBytes), blob.PutOptions{}); err != nil {
			return errors.Wrap(err, "unable to write retention blob")
		}
	}

	if err := writeFormatBlob(ctx, r.blobs, f); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	// remove cached kopia.repository blob.
	if cd := r.cachingOptions.CacheDirectory; cd != "" {
		if err := os.Remove(filepath.Join(r.cachingOptions.CacheDirectory, "kopia.repository")); err != nil {
			log(ctx).Errorf("unable to remove kopia.repository: %v", err)
		}
	}

	return nil
}
