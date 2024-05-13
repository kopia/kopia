package crypto

import (
	"crypto/sha256"

	"github.com/pkg/errors"
	"golang.org/x/crypto/pbkdf2"
)

const (
	// Pbkdf2Algorithm is the key for the pbkdf algorithm.
	Pbkdf2Algorithm = "pbkdf2-sha256-600000"

	// A good rule of thumb is to use a salt that is the same size
	// as the output of the hash function. For example, the output of SHA256
	// is 256 bits (32 bytes), so the salt should be at least 32 random bytes.
	// See: https://crackstation.net/hashing-security.htm
	//
	// However, the NIST recommended minimum size for a salt for pbkdf2 is 16 bytes.
	pbkdf2Sha256MinSaltLength = 16 // 128 bits

	// The NIST recommended iterations for PBKDF2 with SHA256 hash is 600,000.
	pbkdf2Sha256Iterations = 600_000
)

func init() {
	registerPBKeyDeriver(Pbkdf2Algorithm, &pbkdf2KeyDeriver{
		iterations:    pbkdf2Sha256Iterations,
		minSaltLength: pbkdf2Sha256MinSaltLength,
	})
}

type pbkdf2KeyDeriver struct {
	iterations    int
	minSaltLength int
}

func (s *pbkdf2KeyDeriver) deriveKeyFromPassword(password string, salt []byte, keySize int) ([]byte, error) {
	if len(salt) < s.minSaltLength {
		return nil, errors.Errorf("required salt size is atleast %d bytes", s.minSaltLength)
	}

	return pbkdf2.Key([]byte(password), salt, s.iterations, keySize, sha256.New), nil
}
