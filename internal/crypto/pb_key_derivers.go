package crypto

import (
	"fmt"

	"github.com/pkg/errors"
)

// passwordBasedKeyDeriver is an interface that contains methods for deriving a key from a password.
type passwordBasedKeyDeriver interface {
	deriveKeyFromPassword(password string, salt []byte, keySize int) ([]byte, error)
}

//nolint:gochecknoglobals
var keyDerivers = map[string]passwordBasedKeyDeriver{}

// registerPBKeyDeriver registers a password-based key deriver.
func registerPBKeyDeriver(name string, keyDeriver passwordBasedKeyDeriver) {
	if _, ok := keyDerivers[name]; ok {
		panic(fmt.Sprintf("key deriver (%s) is already registered", name))
	}

	keyDerivers[name] = keyDeriver
}

// DeriveKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func DeriveKeyFromPassword(password string, salt []byte, keySize int, algorithm string) ([]byte, error) {
	kd, ok := keyDerivers[algorithm]
	if !ok {
		return nil, errors.Errorf("unsupported key derivation algorithm: %v, supported algorithms %v", algorithm, supportedPBKeyDerivationAlgorithms())
	}

	return kd.deriveKeyFromPassword(password, salt, keySize)
}

// supportedPBKeyDerivationAlgorithms returns a slice of the allowed key derivation algorithms.
func supportedPBKeyDerivationAlgorithms() []string {
	kdAlgorithms := make([]string, 0, len(keyDerivers))
	for k := range keyDerivers {
		kdAlgorithms = append(kdAlgorithms, k)
	}

	return kdAlgorithms
}
