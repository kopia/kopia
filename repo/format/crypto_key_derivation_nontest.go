//go:build !testing
// +build !testing

package format

import (
	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

// DefaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
const DefaultKeyDerivationAlgorithm = "scrypt-65536-8-1"

// DeriveFormatEncryptionKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func (f *KopiaRepositoryJSON) DeriveFormatEncryptionKeyFromPassword(password string) ([]byte, error) {
	const masterKeySize = 32

	switch f.KeyDerivationAlgorithm {
	case "scrypt-65536-8-1":
		// nolint:wrapcheck,gomnd
		return scrypt.Key([]byte(password), f.UniqueID, 65536, 8, 1, masterKeySize)

	default:
		return nil, errors.Errorf("unsupported key algorithm: %v", f.KeyDerivationAlgorithm)
	}
}
