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

type pbkdf2sha256 struct{}

func (p pbkdf2sha256) IsValidSalt(salt []byte) error {
	if len(salt) < minPbkdfSaltSize {
		return fmt.Errorf("required salt size is atleast %d bytes", minPbkdfSaltSize)
	}
	return nil
}

func (p pbkdf2sha256) DeriveKeyFromPassword(password string, salt []byte) ([]byte, error) {
	return pbkdf2.Key([]byte(password), salt, pbkdf2Sha256Iterations, MasterKeyLength, sha256.New), nil
}

func init() {
	Register(Pbkdf2Algorithm, pbkdf2sha256{})
}
