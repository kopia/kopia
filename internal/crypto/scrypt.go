package crypto

import (
	"fmt"

	"golang.org/x/crypto/scrypt"
)

// The recommended minimum size for a salt to be used for scrypt
const minScryptSaltSize = 16 // size in bytes == 128 bits

type scrypt6553681 struct{}

func (s scrypt6553681) IsValidSalt(salt []byte) error {
	if len(salt) < minScryptSaltSize {
		return fmt.Errorf("required salt size is atleast %d bytes", minPbkdfSaltSize)
	}
	return nil
}

func (s scrypt6553681) DeriveKeyFromPassword(password string, salt []byte) ([]byte, error) {
	return scrypt.Key([]byte(password), salt, 65536, 8, 1, MasterKeyLength)
}

func init() {
	Register(ScryptAlgorithm, scrypt6553681{})
}
