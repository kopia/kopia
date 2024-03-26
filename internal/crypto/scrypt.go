//go:build !testing
// +build !testing

package crypto

import (
	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

// The recommended minimum size for a salt to be used for scrypt.
// Currently set to 16 bytes (128 bits).
//
// TBD: A good rule of thumb is to use a salt that is the same size
// as the output of the hash function. For example, the output of SHA256
// is 256 bits (32 bytes), so the salt should be at least 32 random bytes.
// Scrypt uses a SHA256 hash function.
// https://crackstation.net/hashing-security.htm
const minScryptSha256SaltSize = 16 // size in bytes == 128 bits

func init() {
	RegisterKeyDerivationFunc(ScryptAlgorithm, func(password string, salt []byte) ([]byte, error) {
		if len(salt) < minScryptSha256SaltSize {
			return nil, errors.Errorf("required salt size is atleast %d bytes", minPbkdfSha256SaltSize)
		}

		//nolint:gomnd
		return scrypt.Key([]byte(password), salt, 65536, 8, 1, MasterKeyLength)
	})
}
