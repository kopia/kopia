//go:build testing
// +build testing

package repo

import (
	"crypto/sha256"

	"github.com/pkg/errors"
)

// defaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
const defaultKeyDerivationAlgorithm = "testing-only-insecure"

func (f *formatBlob) deriveFormatEncryptionKeyFromPassword(password string) ([]byte, error) {
	const masterKeySize = 32

	switch f.KeyDerivationAlgorithm {
	case defaultKeyDerivationAlgorithm:
		h := sha256.New()
		if _, err := h.Write([]byte(password)); err != nil {
			return nil, err
		}

		return h.Sum(nil), nil

	default:
		return nil, errors.Errorf("unsupported key algorithm: %v", f.KeyDerivationAlgorithm)
	}
}
