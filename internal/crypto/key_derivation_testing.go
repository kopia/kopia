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
	DefaultHashVersion            = 99

	// MasterKeyLength describes the length of the master key.
	MasterKeyLength = 32

	V1SaltLength = 32

	ScryptAlgorithm   = "scrypt-65536-8-1"
	ScryptHashVersion = 1

	Pbkdf2Algorithm   = "pbkdf2-sha256-600000"
	Pbkdf2HashVersion = 2
)

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

func GetPasswordHashVersion(algorithm string) int {
	switch algorithm {
	case DefaultKeyDerivationAlgorithm:
		return DefaultHashVersion
	case ScryptAlgorithm:
		return ScryptHashVersion
	case Pbkdf2Algorithm:
		return Pbkdf2HashVersion
	default:
		return 0
	}
}

func GetPasswordHashAlgorithm(version int) string {
	switch version {
	case DefaultHashVersion:
		return DefaultKeyDerivationAlgorithm
	case ScryptHashVersion:
		return ScryptAlgorithm
	case Pbkdf2HashVersion:
		return Pbkdf2Algorithm
	default:
		return ""
	}
}
