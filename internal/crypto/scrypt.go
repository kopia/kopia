package crypto

import (
	"fmt"

	"golang.org/x/crypto/scrypt"
)

// The recommended minimum size for a salt to be used for scrypt
const minScryptSaltSize = 16 // size in bytes == 128 bits

func init() {
	RegisterKeyDerivationFunc(ScryptAlgorithm, func(password string, salt []byte) ([]byte, error) {
		if len(salt) < minScryptSaltSize {
			return nil, fmt.Errorf("required salt size is atleast %d bytes", minPbkdfSaltSize)
		}
		return scrypt.Key([]byte(password), salt, 65536, 8, 1, MasterKeyLength)
	})
}
