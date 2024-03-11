package crypto

import (
	"fmt"

	"golang.org/x/crypto/scrypt"
)

// The recommended minimum size for a salt to be used for scrypt
// A good rule of thumb is to use a salt that is the same size
// as the output of the hash function. For example, the output of SHA256
// is 256 bits (32 bytes), so the salt should be at least 32 random bytes.
// Scrypt uses a SHA256 hash function.
// https://crackstation.net/hashing-security.htm
const minScryptSha256SaltSize = 32 // size in bytes == 256 bits

func init() {
	RegisterKeyDerivationFunc(ScryptAlgorithm, func(password string, salt []byte) ([]byte, error) {
		if len(salt) < minScryptSha256SaltSize {
			return nil, fmt.Errorf("required salt size is atleast %d bytes", minPbkdfSha256SaltSize)
		}
		return scrypt.Key([]byte(password), salt, 65536, 8, 1, MasterKeyLength)
	})
}
