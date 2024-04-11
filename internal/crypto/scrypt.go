//go:build !testing
// +build !testing

package crypto

import (
	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

// The recommended minimum size for a salt to be used for scrypt.
// Currently set to 16 bytes (128 bits).
//
// TBD: A good rule of thumb is to use a salt that is the same size
// as the output of the hash function. For example, the output of SHA256
// is 256 bits (32 bytes), so the salt should be at least 32 random bytes.
// Scrypt uses a SHA256 hash function.
// https://crackstation.net/hashing-security.htm
const (
	minScryptSha256SaltSize = 16 // size in bytes == 128 bits

	// ScryptAlgorithm is the key for the scrypt algorithm.
	ScryptAlgorithm = "scrypt-65536-8-1"

	// Legacy hash version salt length.
	V1SaltLength = 32

	// Legacy hash version system translates to KeyDerivationAlgorithm.
	HashVersion1 = 1 // this translates to Scrypt KeyDerivationAlgorithm

)

func init() {
	RegisterKeyDerivers(ScryptAlgorithm, &scryptKeyDeriver{
		n:                     65536, //nolint:gomnd
		r:                     8,     //nolint:gomnd
		p:                     1,
		recommendedSaltLength: V1SaltLength,
		minSaltLength:         minScryptSha256SaltSize,
	})
}

type scryptKeyDeriver struct {
	// n scryptCostParameterN is scrypt's CPU/memory cost parameter.
	n int
	// r scryptCostParameterR is scrypt's work factor.
	r int
	// p scryptCostParameterP is scrypt's parallelization parameter.
	p int

	recommendedSaltLength int
	minSaltLength         int
}

func (s *scryptKeyDeriver) DeriveKeyFromPassword(password string, salt []byte) ([]byte, error) {
	if len(salt) < s.minSaltLength {
		return nil, errors.Errorf("required salt size is atleast %d bytes", s.minSaltLength)
	}
	//nolint:wrapcheck
	return scrypt.Key([]byte(password), salt, s.n, s.r, s.p, MasterKeyLength)
}

func (s *scryptKeyDeriver) RecommendedSaltLength() int {
	return s.recommendedSaltLength
}
