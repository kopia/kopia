package crypto

import (
	"golang.org/x/crypto/scrypt"
)

func init() {
	Register(ScryptAlgorithm, func(password string, salt []byte, keyLen int) ([]byte, error) {
		return scrypt.Key([]byte(password), salt, 65536, 8, 1, keyLen)
	})
}
