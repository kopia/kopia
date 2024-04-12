//go:build !testing
// +build !testing

package crypto

import (
	"fmt"

	"github.com/pkg/errors"
)

const (
	// MasterKeyLength describes the length of the master key.
	MasterKeyLength = 32
)

// DefaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
const DefaultKeyDerivationAlgorithm = ScryptAlgorithm

// KeyDerivationParameters  encapsulates all Key Derivation parameters.
type KeyDerivationParameters interface {
	GetKeyDerivationAlgorithm() string
}

// KeyDeriver is an interface that contains methods for deriving a key from a password.
type KeyDeriver interface {
	DeriveKeyFromPassword(password string, salt []byte) ([]byte, error)
	RecommendedSaltLength() int
}

//nolint:gochecknoglobals
var keyDerivers = map[string]KeyDeriver{}

// RegisterKeyDerivers registers various key derivation functions.
func RegisterKeyDerivers(name string, keyDeriver KeyDeriver) {
	if _, ok := keyDerivers[name]; ok {
		panic(fmt.Sprintf("key deriver (%s) is already registered", name))
	}

	keyDerivers[name] = keyDeriver
}

// DeriveKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func DeriveKeyFromPassword(password string, salt []byte, algorithm string) ([]byte, error) {
	kd, ok := keyDerivers[algorithm]
	if !ok {
		return nil, errors.Errorf("unsupported key algorithm: %v, supported algorithms %v", algorithm, AllowedKeyDerivationAlgorithms())
	}

	//nolint:wrapcheck
	return kd.DeriveKeyFromPassword(password, salt)
}

// RecommendedSaltLength returns the recommended salt length of a given key derivation algorithm.
func RecommendedSaltLength(algorithm string) (int, error) {
	kd, ok := keyDerivers[algorithm]
	if !ok {
		return 0, errors.Errorf("unsupported key algorithm: %v, supported algorithms %v", algorithm, AllowedKeyDerivationAlgorithms())
	}

	return kd.RecommendedSaltLength(), nil
}

// AllowedKeyDerivationAlgorithms returns a slice of the allowed key derivation algorithms.
func AllowedKeyDerivationAlgorithms() []string {
	kdAlgorithms := make([]string, 0, len(keyDerivers))
	for k := range keyDerivers {
		kdAlgorithms = append(kdAlgorithms, k)
	}

	return kdAlgorithms
}
