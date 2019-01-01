package repo

import (
	"context"
	"fmt"
)

// Upgrade upgrades repository data structures to the latest version.
func (r *Repository) Upgrade(ctx context.Context) error {
	f := r.formatBlock

	log.Debug("decrypting format...")
	repoConfig, err := f.decryptFormatBytes(r.masterKey)
	if err != nil {
		return fmt.Errorf("unable to decrypt repository config: %v", err)
	}

	var migrated bool

	if repoConfig.FormattingOptions.LegacyBlockFormat != "" {
		log.Infof("upgrading from legacy block format to explicit hash/encryption spec")
		switch repoConfig.FormattingOptions.LegacyBlockFormat {
		case "UNENCRYPTED_HMAC_SHA256":
			repoConfig.FormattingOptions.Hash = "HMAC-SHA256"
			repoConfig.FormattingOptions.Encryption = "NONE"
		case "UNENCRYPTED_HMAC_SHA256_128":
			repoConfig.FormattingOptions.Hash = "HMAC-SHA256-128"
			repoConfig.FormattingOptions.Encryption = "NONE"
		case "ENCRYPTED_HMAC_SHA256_AES256_SIV":
			repoConfig.FormattingOptions.Hash = "HMAC-SHA256-128"
			repoConfig.FormattingOptions.Encryption = "AES-256-CTR"
		}
		repoConfig.FormattingOptions.LegacyBlockFormat = ""
		migrated = true
	}

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
