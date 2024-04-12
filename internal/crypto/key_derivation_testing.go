//go:build testing
// +build testing

package crypto

import (
	"crypto/sha256"

	"github.com/pkg/errors"
)

const (
	// DefaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
	DefaultKeyDerivationAlgorithm = "testing-only-insecure"

	// MasterKeyLength describes the length of the master key.
	MasterKeyLength = 32

	V1SaltLength    = 32
	HashVersion1    = 1 // this translates to Scrypt KeyDerivationAlgorithm
	ScryptAlgorithm = "scrypt-65536-8-1"
	Pbkdf2Algorithm = "pbkdf2"
)

type Parameters interface {
	GetKeyDerivationAlgorithm() string
}

// DeriveKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func DeriveKeyFromPassword(password string, salt []byte, algorithm string) ([]byte, error) {
	const masterKeySize = 32

	switch algorithm {
	case DefaultKeyDerivationAlgorithm, ScryptAlgorithm, Pbkdf2Algorithm:
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

func RecommendedSaltLength(algorithm string) (int, error) {
	return V1SaltLength, nil
}

func AllowedKeyDerivationAlgorithms() []string {
	return []string{DefaultKeyDerivationAlgorithm}
}
