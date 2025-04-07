package crypto

import (
	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

const (
	// ScryptAlgorithm is the registration name for the scrypt algorithm instance.
	ScryptAlgorithm = "scrypt-65536-8-1"

	// The recommended minimum size for a salt to be used for scrypt.
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
