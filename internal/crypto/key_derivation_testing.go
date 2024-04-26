//go:build testing
// +build testing

package crypto

import (
	"crypto/sha256"

	"github.com/pkg/errors"
)

const (
	// MasterKeyLength describes the length of the master key.
	MasterKeyLength = 32

	ScryptAlgorithm = "scrypt-65536-8-1"

	Pbkdf2Algorithm = "pbkdf2-sha256-600000"

	// DefaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
	DefaultKeyDerivationAlgorithm = ScryptAlgorithm
)

// DeriveKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func DeriveKeyFromPassword(password string, salt []byte, algorithm string) ([]byte, error) {
	const masterKeySize = 32

	switch algorithm {
	case ScryptAlgorithm, Pbkdf2Algorithm:
		h := sha256.New()
		// Adjust password so that we get a different key for each algorithm
		if _, err := h.Write([]byte(password + algorithm)); err != nil {
			return nil, err
		}

		return h.Sum(nil), nil

	default:
		return nil, errors.Errorf("unsupported key algorithm: %v", algorithm)
	}
}

func AllowedKeyDerivationAlgorithms() []string {
	return []string{DefaultKeyDerivationAlgorithm}
}
