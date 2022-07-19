package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

// FormatBlobBackupIDPrefix is the prefix for all identifiers of the BLOBs that
// keep a backup copy of the FormatBlobID BLOB for the purposes of rollback
// during upgrade.
const FormatBlobBackupIDPrefix = "kopia.repository.backup."

// FormatBlobBackupID gets the upgrade backu pblob-id fro mthe lock.
func FormatBlobBackupID(l UpgradeLockIntent) blob.ID {
	return blob.ID(FormatBlobBackupIDPrefix + l.OwnerID)
}

func (r *directRepository) updateRepoConfig(ctx context.Context, cb func(repoConfig *repositoryObjectFormat) error) (*repositoryObjectFormat, error) {
	f := r.formatBlob

	repoConfig, err := f.decryptFormatBytes(r.formatEncryptionKey)
	if err != nil {
		return nil, errors.Wrap(err, "unable to decrypt repository config")
	}

	if err := cb(repoConfig); err != nil {
		return nil, err
	}

	if err := encryptFormatBytes(f, repoConfig, r.formatEncryptionKey, f.UniqueID); err != nil {
		return nil, errors.Errorf("unable to encrypt format bytes")
	}

	if err := writeFormatBlob(ctx, r.blobs, f, r.blobCfgBlob); err != nil {
		return nil, errors.Wrap(err, "unable to write format blob")
	}

	if cd := r.cachingOptions.CacheDirectory; cd != "" {
		if err := os.Remove(filepath.Join(cd, FormatBlobID)); err != nil && !os.IsNotExist(err) {
			return nil, errors.Errorf("unable to remove cached repository format blob: %v", err)
		}
	}

	return repoConfig, nil
}

// SetUpgradeLockIntent sets the upgrade lock intent on the repository format
// blob for other clients to notice. If a lock intent was already placed then
// it updates the existing lock using the output of the UpgradeLock.Update().
//
// This method also backs up the original format version on the upgrade lock
// intent and sets the latest format-version o nthe repository blob. This
// should cause the unsupporting clients (non-upgrade capable) to fail
// connecting to the repository.
func (r *directRepository) SetUpgradeLockIntent(ctx context.Context, l UpgradeLockIntent) (*UpgradeLockIntent, error) {
	repoConfig, err := r.updateRepoConfig(ctx, func(repoConfig *repositoryObjectFormat) error {
		if err := l.Validate(); err != nil {
			return errors.Wrap(err, "invalid upgrade lock intent")
		}

		if repoConfig.UpgradeLock == nil {
			// when we are putting a new lock then ensure that we can upgrade
			// to that version
			if repoConfig.FormattingOptions.Version >= content.MaxFormatVersion {
				return errors.Errorf("repository is using version %d, and version %d is the maximum",
					repoConfig.FormattingOptions.Version, content.MaxFormatVersion)
			}

			// backup the current repository config from local cache to the
			// repository when we place the lock for the first time
			if err := writeFormatBlobWithID(ctx, r.blobs, r.formatBlob, r.blobCfgBlob, FormatBlobBackupID(l)); err != nil {
				return errors.Wrap(err, "failed to backup the repo format blob")
			}

			// set a new lock or revoke an existing lock
			repoConfig.UpgradeLock = &l
			// mark the upgrade to the new format version, this will ensure that older
			// clients won't be able to parse the new version
			repoConfig.FormattingOptions.Version = content.MaxFormatVersion
		} else if newL, err := repoConfig.UpgradeLock.Update(&l); err == nil {
			repoConfig.UpgradeLock = newL
		} else {
			return errors.Wrap(err, "failed to update the existing lock")
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return repoConfig.UpgradeLock, nil
}

// CommitUpgrade removes the upgrade lock from the from the repository format
// blob. This in-effect commits the new repository format t othe repository and
// resumes all access to the repository.
func (r *directRepository) CommitUpgrade(ctx context.Context) error {
	_, err := r.updateRepoConfig(ctx, func(repoConfig *repositoryObjectFormat) error {
		if repoConfig.UpgradeLock == nil {
			return errors.New("no upgrade in progress")
		}

		// restore the old format version
		repoConfig.UpgradeLock = nil

		return nil
	})

	return err
}

// RollbackUpgrade removes the upgrade lock while also restoring the
// format-blob's original version. This method does not restore the original
// repository data format and neither does it validate against any repository
// changes. Rolling back the repository format is currently not supported and
// hence using this API could render the repository corrupted and unreadable by
// clients.
//
// nolint:gocyclo
func (r *directRepository) RollbackUpgrade(ctx context.Context) error {
	f := r.formatBlob

	repoConfig, err := f.decryptFormatBytes(r.formatEncryptionKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	if repoConfig.UpgradeLock == nil {
		return errors.New("no upgrade in progress")
	}

	// restore the oldest backup and delete the rest
	var oldestBackup *blob.Metadata

	if err = r.blobs.ListBlobs(ctx, FormatBlobBackupIDPrefix, func(bm blob.Metadata) error {
		var delID blob.ID
		if oldestBackup == nil || bm.Timestamp.Before(oldestBackup.Timestamp) {
			if oldestBackup != nil {
				// delete the current candidate because we have found an even older one
				delID = oldestBackup.BlobID
			}
			oldestBackup = &bm
		} else {
			delID = bm.BlobID
		}

		if delID != "" {
			// delete the backup that we are not going to need for rollback
			if err = r.blobs.DeleteBlob(ctx, delID); err != nil {
				return errors.Wrapf(err, "failed to delete the format blob backup %q", delID)
			}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to list backup blobs")
	}

	// restore only when we find a backup, otherwise simply cleanup the local cache
	if oldestBackup != nil {
		var d gather.WriteBuffer
		if err = r.blobs.GetBlob(ctx, oldestBackup.BlobID, 0, -1, &d); err != nil {
			return errors.Wrapf(err, "failed to read from backup %q", oldestBackup.BlobID)
		}

		if err = r.blobs.PutBlob(ctx, FormatBlobID, d.Bytes(), blob.PutOptions{}); err != nil {
			return errors.Wrapf(err, "failed to restore format blob from backup %q", oldestBackup.BlobID)
		}

		// delete the backup after we have restored the format-blob
		if err = r.blobs.DeleteBlob(ctx, oldestBackup.BlobID); err != nil {
			return errors.Wrapf(err, "failed to delete the format blob backup %q", oldestBackup.BlobID)
		}
	}

	if cd := r.cachingOptions.CacheDirectory; cd != "" {
		if err = os.Remove(filepath.Join(cd, FormatBlobID)); err != nil && !os.IsNotExist(err) {
			return errors.Errorf("unable to remove cached repository format blob: %v", err)
		}
	}

	return nil
}
