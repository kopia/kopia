package crypto

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

const (
	// ScryptAlgorithm is the full algorithm name for scrypt.
	ScryptAlgorithm = "scrypt-65536-8-1"

	// Scrypt is the short name for scrypt, used in CLI flags.
	Scrypt = "scrypt"

	// scryptMinSaltLength is the recommended minimum size for a salt for scrypt.
	// Currently set to 16 bytes (128 bits).
	//
	// A good rule of thumb is to use a salt that is the same size
	// as the output of the hash function. For example, the output of SHA256
	// is 256 bits (32 bytes), so the salt should be at least 32 random bytes.
	// Scrypt uses a SHA256 hash function.
	// https://crackstation.net/hashing-security.htm
	scryptMinSaltLength = 16 // 128 bits
)

func init() {
	registerPBKeyDeriver(ScryptAlgorithm, &scryptKeyDeriver{
		n:             65536, //nolint:mnd
		r:             8,     //nolint:mnd
		p:             1,
		minSaltLength: scryptMinSaltLength,
	})
}

// NewScryptKeyDeriverWithMemory creates a new scrypt key deriver with the specified memory cost in MB.
// The scrypt N parameter is calculated as: N = memMB * 1024 (since r=8 and block size=128)
// The algorithm name is unique to the memory setting, allowing multiple configurations to coexist.
// If the algorithm is already registered, returns the existing algorithm name.
func NewScryptKeyDeriverWithMemory(memMB int) string {
	// scrypt: memory = N * r * 128 bytes. With r=8: N = memMB * 1024
	n := memMB * 1024
	algorithmName := fmt.Sprintf("scrypt-%d-8-1", n)

	// Check if already registered
	if _, ok := keyDerivers[algorithmName]; ok {
		return algorithmName
	}

	registerPBKeyDeriver(algorithmName, &scryptKeyDeriver{
		n:             n,
		r:             8,
		p:             1,
		minSaltLength: scryptMinSaltLength,
	})

	return algorithmName
}

type scryptKeyDeriver struct {
	// n scryptCostParameterN is scrypt's CPU/memory cost parameter.
	n int
	// r scryptCostParameterR is scrypt's work factor.
	r int
	// p scryptCostParameterP is scrypt's parallelization parameter.
	p int

	minSaltLength int
}

func (s *scryptKeyDeriver) deriveKeyFromPassword(password string, salt []byte, keySize int) ([]byte, error) {
	if len(salt) < s.minSaltLength {
		return nil, errors.Errorf("required salt size is at least %d bytes", s.minSaltLength)
	}
	//nolint:wrapcheck
	return scrypt.Key([]byte(password), salt, s.n, s.r, s.p, keySize)
}
