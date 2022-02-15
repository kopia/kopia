package repo

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/content"
)

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
func (r *directRepository) SetUpgradeLockIntent(ctx context.Context, l content.UpgradeLock) (*content.UpgradeLock, error) {
	repoConfig, err := r.updateRepoConfig(ctx, func(repoConfig *repositoryObjectFormat) error {
		if err := l.Validate(); err != nil {
			return errors.Wrap(err, "invalid upgrade lock intent")
		}

		if repoConfig.FormattingOptions.UpgradeLock == nil {
			// when we are putting a new lock then ensure that we can upgrade
			// to that version
			if repoConfig.FormattingOptions.Version >= content.MaxFormatVersion {
				return errors.Errorf("repository is using version %d, and version %d is the maximum",
					repoConfig.FormattingOptions.Version, content.MaxFormatVersion)
			}
			// backup the old format version
			l.OldFormatVersion = repoConfig.FormattingOptions.Version
			// set a new lock or revoke an existing lock
			repoConfig.FormattingOptions.UpgradeLock = &l
			// mark the upgrade to the new format version, this will ensure that older
			// clients won't be able to parse the new version
			repoConfig.FormattingOptions.Version = content.MaxFormatVersion
		} else if newL, err := repoConfig.FormattingOptions.UpgradeLock.Update(&l); err == nil {
			repoConfig.FormattingOptions.UpgradeLock = newL
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
		if repoConfig.FormattingOptions.UpgradeLock == nil {
			return errors.New("no upgrade in progress")
		}

		// restore the old format version
		repoConfig.FormattingOptions.UpgradeLock = nil

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
func (r *directRepository) RollbackUpgrade(ctx context.Context) error {
	_, err := r.updateRepoConfig(ctx, func(repoConfig *repositoryObjectFormat) error {
		if repoConfig.FormattingOptions.UpgradeLock == nil {
			return errors.New("no upgrade in progress")
		}

		// restore the old format version
		repoConfig.FormattingOptions.Version = repoConfig.UpgradeLock.OldFormatVersion
		repoConfig.FormattingOptions.UpgradeLock = nil

		return nil
	})

	return err
}
