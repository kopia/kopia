package crypto

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// The NIST recommended iterations for PBKDF2 with SHA256 hash is 600,000
const pbkdf2Sha256Iterations = 1<<20 - 1<<18 // 786,432

// The NIST recommended minimum size for a salt for pbkdf2 is 16 bytes.
// However, A good rule of thumb is to use a salt that is the same size
// as the output of the hash function. For example, the output of SHA256
// is 256 bits (32 bytes), so the salt should be at least 32 random bytes
// https://crackstation.net/hashing-security.htm
const minPbkdfSha256SaltSize = 32 // size in bytes == 256 bits

func init() {
	RegisterKeyDerivationFunc(Pbkdf2Algorithm, func(password string, salt []byte) ([]byte, error) {
		if len(salt) < minPbkdfSha256SaltSize {
			return nil, fmt.Errorf("required salt size is atleast %d bytes", minPbkdfSha256SaltSize)
		}
		return pbkdf2.Key([]byte(password), salt, pbkdf2Sha256Iterations, MasterKeyLength, sha256.New), nil
	})
}
