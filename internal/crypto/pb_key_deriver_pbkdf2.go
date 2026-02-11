package crypto

import (
	"crypto/pbkdf2"
	"crypto/sha256"
	"fmt"

	"github.com/pkg/errors"
)

const (
	// Pbkdf2Algorithm is the full algorithm name for PBKDF2.
	Pbkdf2Algorithm = "pbkdf2-sha256-600000"

	// PBKDF2 is the short name for PBKDF2, used in CLI flags.
	PBKDF2 = "pbkdf2"

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

// NewPBKDF2KeyDeriverWithIterations creates a new PBKDF2 key deriver with the specified number of iterations.
// The algorithm name is unique to the iteration count, allowing multiple configurations to coexist.
// If the algorithm is already registered, returns the existing algorithm name.
func NewPBKDF2KeyDeriverWithIterations(iterations int) string {
	algorithmName := fmt.Sprintf("pbkdf2-sha256-%d", iterations)

	// Check if already registered
	if _, ok := keyDerivers[algorithmName]; ok {
		return algorithmName
	}

	registerPBKeyDeriver(algorithmName, &pbkdf2KeyDeriver{
		iterations:    iterations,
		minSaltLength: pbkdf2Sha256MinSaltLength,
	})

	return algorithmName
}

type pbkdf2KeyDeriver struct {
	iterations    int
	minSaltLength int
}

func (s *pbkdf2KeyDeriver) deriveKeyFromPassword(password string, salt []byte, keySize int) ([]byte, error) {
	if len(salt) < s.minSaltLength {
		return nil, errors.Errorf("required salt size is atleast %d bytes", s.minSaltLength)
	}

	derivedKey, err := pbkdf2.Key(sha256.New, password, salt, s.iterations, keySize)
	if err != nil {
		return nil, errors.Wrap(err, "unable to derive key")
	}

	return derivedKey, nil
}
