//go:build !testing
// +build !testing

package repo

import (
	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

// defaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
const defaultKeyDerivationAlgorithm = "scrypt-65536-8-1"

func (f *formatBlob) deriveFormatEncryptionKeyFromPassword(password string) ([]byte, error) {
	const masterKeySize = 32

	switch f.KeyDerivationAlgorithm {
	case "scrypt-65536-8-1":
		// nolint:wrapcheck,gomnd
		return scrypt.Key([]byte(password), f.UniqueID, 65536, 8, 1, masterKeySize)

	default:
		return nil, errors.Errorf("unsupported key algorithm: %v", f.KeyDerivationAlgorithm)
	}
}
