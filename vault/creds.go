package vault

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/scrypt"

	"golang.org/x/crypto/pbkdf2"
)

const (
	passwordBasedKeySize = 32

	// MinPasswordLength is the minimum allowed length of a password in charcters.
	MinPasswordLength = 12

	// MinMasterKeyLength is the minimum allowed length of a master key, in bytes.
	MinMasterKeyLength = 16

	defaultKeyAlgorithm = "scrypt-65536-8-1"
)

var SupportedKeyAlgorithms = []string{
	"scrypt-65536-8-1",
	"pbkdf2-sha256-100000",
}

// Credentials encapsulates credentials used to encrypt a Vault.
type Credentials interface {
	getMasterKey(f *Format) ([]byte, error)
}

type masterKeyCredentials struct {
	key []byte
}

func (mkc *masterKeyCredentials) getMasterKey(f *Format) ([]byte, error) {
	return mkc.key, nil
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

func (pc *passwordCredentials) getMasterKey(f *Format) ([]byte, error) {
	switch f.KeyAlgo {
	case "pbkdf2-sha256-100000":
		return pbkdf2.Key([]byte(pc.password), f.UniqueID, 100000, passwordBasedKeySize, sha256.New), nil

	case "scrypt-65536-8-1":
		return scrypt.Key([]byte(pc.password), f.UniqueID, 65536, 8, 1, passwordBasedKeySize)

	default:
		return nil, fmt.Errorf("unsupported key algorithm: %v", f.KeyAlgo)
	}
}

// Password returns password-based Credentials with the specified password.
func Password(password string) (Credentials, error) {
	if len(password) < MinPasswordLength {
		return nil, fmt.Errorf("password too short")
	}

	return &passwordCredentials{password}, nil
}
