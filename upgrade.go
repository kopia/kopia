package repo

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
)

// Upgrade upgrades repository data structures to the latest version.
func (r *Repository) Upgrade(ctx context.Context) error {
	f := r.formatBlock

	log.Debug("decrypting format...")
	repoConfig, err := f.decryptFormatBytes(r.masterKey)
	if err != nil {
		return errors.Wrap(err, "unable to decrypt repository config")
	}

	var migrated bool

	// TODO(jkowalski): add migration code here
	if !migrated {
		log.Infof("nothing to do")
		return nil
	}

	log.Debug("encrypting format...")
	if err := encryptFormatBytes(f, repoConfig, r.masterKey, f.UniqueID); err != nil {
		return fmt.Errorf("unable to encrypt format bytes")
	}

	log.Infof("writing updated format block...")
	return writeFormatBlock(ctx, r.Storage, f)
}
