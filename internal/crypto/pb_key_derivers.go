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

// registerAlgorithmIfNeeded registers a KDF algorithm by its name if it matches a known pattern.
func registerAlgorithmIfNeeded(algorithm string) {
	// Check if already registered
	if _, ok := keyDerivers[algorithm]; ok {
		return
	}

	// Parse PBKDF2 algorithm: pbkdf2-sha256-{iterations}
	var pbkdf2Iterations int
	if _, err := fmt.Sscanf(algorithm, "pbkdf2-sha256-%d", &pbkdf2Iterations); err == nil && pbkdf2Iterations > 0 {
		keyDerivers[algorithm] = &pbkdf2KeyDeriver{
			iterations:    pbkdf2Iterations,
			minSaltLength: pbkdf2Sha256MinSaltLength,
		}
		return
	}

	// Parse scrypt algorithm: scrypt-{N}-8-1
	var n int
	if _, err := fmt.Sscanf(algorithm, "scrypt-%d-8-1", &n); err == nil && n > 0 {
		keyDerivers[algorithm] = &scryptKeyDeriver{
			n:             n,
			r:             8,
			p:             1,
			minSaltLength: scryptMinSaltLength,
		}
		return
	}
}

// DeriveKeyFromPassword derives encryption key using the provided password and per-repository unique ID.
func DeriveKeyFromPassword(password string, salt []byte, keySize int, algorithm string) ([]byte, error) {
	// Try to register the algorithm if it follows a known pattern
	registerAlgorithmIfNeeded(algorithm)

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