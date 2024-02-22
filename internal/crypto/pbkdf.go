package crypto

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// The NIST recommended iterations for PBKDF2 with SHA256 hash is 600,000
const pbkdf2Sha256Iterations = 1<<20 - 1<<18 // 786,432

// The NIST recommended minimum size for a salt to be used for pbkdf2
const minPbkdfSaltSize = 16 // size in bytes == 128 bits

func init() {
	RegisterKeyDerivationFunc(Pbkdf2Algorithm, func(password string, salt []byte) ([]byte, error) {
		if len(salt) < minPbkdfSaltSize {
			return nil, fmt.Errorf("required salt size is atleast %d bytes", minPbkdfSaltSize)
		}
		return pbkdf2.Key([]byte(password), salt, pbkdf2Sha256Iterations, MasterKeyLength, sha256.New), nil
	})
}
