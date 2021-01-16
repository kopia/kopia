package repo

import (
	"context"

	"github.com/pkg/errors"
)

// Upgrade upgrades repository data structures to the latest version.
func (r *directRepository) Upgrade(ctx context.Context) error {
	f := r.formatBlob

	repoConfig, err := f.decryptFormatBytes(r.masterKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	var migrated bool

	// add migration code here
	if !migrated {
		log(ctx).Infof("nothing to do")
		return nil
	}

	if err := encryptFormatBytes(f, repoConfig, r.masterKey, f.UniqueID); err != nil {
		return errors.Errorf("unable to encrypt format bytes")
	}

	log(ctx).Infof("writing updated format content...")

	return writeFormatBlob(ctx, r.blobs, f)
}
