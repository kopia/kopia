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

type keyDerivationFunc func(password string, salt []byte) ([]byte, error)

var keyDerivers = map[string]keyDerivationFunc{}

func RegisterKeyDerivationFunc(name string, keyDeriver keyDerivationFunc) {
	keyDerivers[name] = keyDeriver
}

// DeriveKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func DeriveKeyFromPassword(password string, salt []byte, algorithm string) ([]byte, error) {
	kdFunc, ok := keyDerivers[algorithm]
	if !ok {
		return nil, errors.Errorf("unsupported key algorithm: %v", algorithm)
	}
	return kdFunc(password, salt)
}
