package format

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/repo/blob"
)

// ChangePassword changes the repository password and rewrites
// `kopia.repository` & `kopia.blobcfg`.
// If kdf is empty, the existing KDF algorithm is preserved.
// If kdf is "pbkdf2" or "scrypt", the corresponding algorithm is used with the provided parameters.
func (m *Manager) ChangePassword(ctx context.Context, newPassword string, kdf string, kdfIterations, kdfMemoryMB int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.repoConfig.EnablePasswordChange {
		return errors.New("password changes are not supported for repositories created using Kopia v0.8 or older")
	}

	// Determine the KDF algorithm to use
	algorithm := m.j.KeyDerivationAlgorithm
	if kdf != "" {
		// Build algorithm from parameters
		if kdf == crypto.PBKDF2 {
			if kdfIterations > 0 {
				algorithm = crypto.NewPBKDF2KeyDeriverWithIterations(kdfIterations)
			} else {
				algorithm = crypto.Pbkdf2Algorithm
			}
		} else if kdf == crypto.Scrypt {
			if kdfMemoryMB > 0 {
				algorithm = crypto.NewScryptKeyDeriverWithMemory(kdfMemoryMB)
			} else {
				algorithm = crypto.ScryptAlgorithm
			}
		}
	}

	newFormatEncryptionKey, err := crypto.DeriveKeyFromPassword(newPassword, m.j.UniqueID, 32, algorithm)
	if err != nil {
		return errors.Wrap(err, "unable to derive master key")
	}

	m.formatEncryptionKey = newFormatEncryptionKey
	m.password = newPassword

	// Update the key derivation algorithm in the format blob if it changed
	if kdf != "" {
		m.j.KeyDerivationAlgorithm = algorithm
	}

	if err := m.j.EncryptRepositoryConfig(m.repoConfig, newFormatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to encrypt format bytes")
	}

	if err := m.j.WriteBlobCfgBlob(ctx, m.blobs, m.blobCfgBlob, newFormatEncryptionKey); err != nil {
		return errors.Wrap(err, "unable to write blobcfg blob")
	}

	if err := m.j.WriteKopiaRepositoryBlob(ctx, m.blobs, m.blobCfgBlob); err != nil {
		return errors.Wrap(err, "unable to write format blob")
	}

	m.cache.Remove(ctx, []blob.ID{KopiaRepositoryBlobID, KopiaBlobCfgBlobID})

	return nil
}