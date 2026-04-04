package crypto

import (
	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

const (
	// scryptCostParameterN, scryptCostParameterR, and scryptCostParameterP
	// are defined in pb_key_deriver_scrypt_32bit.go and pb_key_deriver_scrypt_64bit.go
	// with architecture-specific values to avoid OOM on 32-bit systems.

	// ScryptAlgorithm is the registration name for the scrypt algorithm instance.
	// On 64-bit systems this uses scrypt-65536-8-1 (64MB memory).
	// On 32-bit systems this uses scrypt-16384-4-1 (8MB memory) to avoid OOM.
	//
	// For compatibility, both algorithm names are registered and point to the
	// same implementation with architecture-appropriate parameters.
	ScryptAlgorithm = "scrypt-65536-8-1"

	// Scrypt32BitAlgorithm is an alias for 32-bit compatible scrypt parameters.
	Scrypt32BitAlgorithm = "scrypt-16384-4-1"

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
	// Register the scrypt key deriver with architecture-specific parameters.
	// We register under both algorithm names for compatibility:
	// - The standard name "scrypt-65536-8-1" for existing repositories
	// - The 32-bit name "scrypt-16384-4-1" for 32-bit compatible repositories
	//
	// Both names use the same actual parameters (from scryptCostParameter* constants)
	// which are architecture-dependent.
	deriver := &scryptKeyDeriver{
		n:             scryptCostParameterN,
		r:             scryptCostParameterR,
		p:             scryptCostParameterP,
		minSaltLength: scryptMinSaltLength,
	}

	// Register under the standard name (used by existing code/repos)
	registerPBKeyDeriver(ScryptAlgorithm, deriver)

	// Also register under the 32-bit name for clarity
	registerPBKeyDeriver(Scrypt32BitAlgorithm, deriver)
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
