//go:build !testing
// +build !testing

package crypto

import (
	"github.com/pkg/errors"
)

const (
	MasterKeyLength = 32

	ScryptAlgorithm = "scrypt-65536-8-1"
	Pbkdf2Algorithm = "pbkdf2"
)

// DefaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
const DefaultKeyDerivationAlgorithm = ScryptAlgorithm

type KeyDeriver interface {
	IsValidSalt(salt []byte) error

	DeriveKeyFromPassword(password string, salt []byte) ([]byte, error)
}

func CreateKeyDeriver(algorithm string) (KeyDeriver, error) {
	keyDeriver, ok := keyDerivers[algorithm]
	if !ok {
		return nil, errors.Errorf("unsupported key derivation algorithm: %v", algorithm)
	}
	return keyDeriver, nil
}

type keyDerivationFunc func(password string, salt []byte, keyLen int) ([]byte, error)

var keyDerivers = map[string]KeyDeriver{}

func Register(name string, keyDeriver KeyDeriver) {
	keyDerivers[name] = keyDeriver
}

// DeriveKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func DeriveKeyFromPassword(password string, salt []byte, algorithm string) ([]byte, error) {
	kdFunc, ok := keyDerivationFunctions[algorithm]
	if !ok {
		return nil, errors.Errorf("unsupported key algorithm: %v", algorithm)
	}
	return kdFunc(password, salt, MasterKeyLength)
}
