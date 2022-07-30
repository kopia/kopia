package format

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/feature"
)

// RepositoryConfig describes the format of objects in a repository.
// The contents of this object are stored encrypted since they contain sensitive key material.
type RepositoryConfig struct {
	ContentFormat
	ObjectFormat

	UpgradeLock      *UpgradeLockIntent `json:"upgradeLock,omitempty"`
	RequiredFeatures []feature.Required `json:"requiredFeatures,omitempty"`
}

// EncryptedRepositoryConfig contains the configuration of repository that's persisted in encrypted format.
type EncryptedRepositoryConfig struct {
	Format RepositoryConfig `json:"format"`
}

// DecodedRepositoryConfig encapsulates contents of decoded `kopia.repository` blob.
type DecodedRepositoryConfig struct {
	KopiaRepository      *KopiaRepositoryJSON
	KopiaRepositoryBytes []byte            // serialized format blob
	CacheMTime           time.Time         // mod time of the format blob cache file
	RepoConfig           *RepositoryConfig // unencrypted format blob structure
	FormatEncryptionKey  []byte            // key derived from the password
}

// DecryptRepositoryConfig decrypts RepositoryConfig stored in EncryptedFormatBytes.
func (f *KopiaRepositoryJSON) DecryptRepositoryConfig(masterKey []byte) (*RepositoryConfig, error) {
	switch f.EncryptionAlgorithm {
	case aes256GcmEncryption:
		plainText, err := decryptRepositoryBlobBytesAes256Gcm(f.EncryptedFormatBytes, masterKey, f.UniqueID)
		if err != nil {
			return nil, errors.Errorf("unable to decrypt repository format")
		}

		var erc EncryptedRepositoryConfig
		if err := json.Unmarshal(plainText, &erc); err != nil {
			return nil, errors.Wrap(err, "invalid repository format")
		}

		return &erc.Format, nil

	default:
		return nil, errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}

// EncryptRepositoryConfig encrypts the provided repository config and stores it in EncryptedFormatBytes.
func (f *KopiaRepositoryJSON) EncryptRepositoryConfig(format *RepositoryConfig, masterKey []byte) error {
	switch f.EncryptionAlgorithm {
	case aes256GcmEncryption:
		data, err := json.Marshal(&EncryptedRepositoryConfig{Format: *format})
		if err != nil {
			return errors.Wrap(err, "can't marshal format to JSON")
		}

		data, err = encryptRepositoryBlobBytesAes256Gcm(data, masterKey, f.UniqueID)
		if err != nil {
			return errors.Wrap(err, "failed to encrypt format JSON")
		}

		f.EncryptedFormatBytes = data

		return nil

	default:
		return errors.Errorf("unknown encryption algorithm: '%v'", f.EncryptionAlgorithm)
	}
}
