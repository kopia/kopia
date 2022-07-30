//go:build testing
// +build testing

package format

import (
	"crypto/sha256"

	"github.com/pkg/errors"
)

// DefaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
const DefaultKeyDerivationAlgorithm = "testing-only-insecure"

// DeriveFormatEncryptionKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func (f *KopiaRepositoryJSON) DeriveFormatEncryptionKeyFromPassword(password string) ([]byte, error) {
	const masterKeySize = 32

	switch f.KeyDerivationAlgorithm {
	case DefaultKeyDerivationAlgorithm:
		h := sha256.New()
		if _, err := h.Write([]byte(password)); err != nil {
			return nil, err
		}

		return h.Sum(nil), nil

	default:
		return nil, errors.Errorf("unsupported key algorithm: %v", f.KeyDerivationAlgorithm)
	}
}
