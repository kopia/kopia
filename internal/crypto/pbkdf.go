//go:build !testing
// +build !testing

package crypto

import (
	"crypto/sha256"

	"github.com/pkg/errors"
	"golang.org/x/crypto/pbkdf2"
)

const (
	// The NIST recommended minimum size for a salt for pbkdf2 is 16 bytes.
	//
	// TBD: However, a good rule of thumb is to use a salt that is the same size
	// as the output of the hash function. For example, the output of SHA256
	// is 256 bits (32 bytes), so the salt should be at least 32 random bytes.
	// See: https://crackstation.net/hashing-security.htm
	minPbkdfSha256SaltSize = 32 // size in bytes == 128 bits

	// The NIST recommended iterations for PBKDF2 with SHA256 hash is 600,000.
	pbkdf2Sha256Iterations = 600_000

	// Pbkdf2Algorithm is the key for the pbkdf algorithm.
	Pbkdf2Algorithm = "pbkdf2-sha256-600000"
)

func init() {
	RegisterKeyDerivers(Pbkdf2Algorithm, &pbkdf2KeyDeriver{
		iterations:            pbkdf2Sha256Iterations,
		recommendedSaltLength: minPbkdfSha256SaltSize,
		minSaltLength:         minPbkdfSha256SaltSize,
	})
}

type pbkdf2KeyDeriver struct {
	iterations            int
	recommendedSaltLength int
	minSaltLength         int
}

func (s *pbkdf2KeyDeriver) DeriveKeyFromPassword(password string, salt []byte) ([]byte, error) {
	if len(salt) < s.minSaltLength {
		return nil, errors.Errorf("required salt size is atleast %d bytes", s.minSaltLength)
	}

	return pbkdf2.Key([]byte(password), salt, s.iterations, MasterKeyLength, sha256.New), nil
}

func (s *pbkdf2KeyDeriver) RecommendedSaltLength() int {
	return s.recommendedSaltLength
}
