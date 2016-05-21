package vault

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

const (
	passwordBasedKeySize = 32

	pbkdf2Rounds = 10000

	// MinPasswordLength is the minimum allowed length of a password in charcters.
	MinPasswordLength = 12

	// MinMasterKeyLength is the minimum allowed length of a master key, in bytes.
	MinMasterKeyLength = 16
)

// Credentials em
type Credentials interface {
	getMasterKey(salt []byte) []byte
}

type masterKeyCredentials struct {
	key []byte
}

func (mkc *masterKeyCredentials) getMasterKey(salt []byte) []byte {
	return mkc.key
}

// MasterKey returns master key-based Credentials with the specified key.
func MasterKey(key []byte) (Credentials, error) {
	if len(key) < MinMasterKeyLength {
		return nil, fmt.Errorf("master key too short")
	}

	return &masterKeyCredentials{key}, nil
}

type passwordCredentials struct {
	password string
}

func (pc *passwordCredentials) getMasterKey(salt []byte) []byte {
	return pbkdf2.Key([]byte(pc.password), salt, pbkdf2Rounds, passwordBasedKeySize, sha256.New)
}

// Password returns password-based Credentials with the specified password.
func Password(password string) (Credentials, error) {
	if len(password) < MinPasswordLength {
		return nil, fmt.Errorf("password too short")
	}

	return &passwordCredentials{password}, nil
}
