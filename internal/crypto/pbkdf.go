package crypto

import (
	"crypto/sha256"

	"golang.org/x/crypto/pbkdf2"
)

// The recommended iterations for PBKDF2 with SHA256 hash is 600,000
const pbkdf2Sha256Iterations = 1<<20 - 1<<18 // 786,432

func init() {
	Register(Pbkdf2Algorithm, func(password string, salt []byte, keyLen int) ([]byte, error) {
		return pbkdf2.Key([]byte(password), salt, pbkdf2Sha256Iterations, keyLen, sha256.New), nil
	})
}
