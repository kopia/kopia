package auth

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/crypto/scrypt"
)

const (
	passwordBasedKeySize = 32

	// MinPasswordLength is the minimum allowed length of a password in charcters.
	MinPasswordLength = 12

	// MinMasterKeyLength is the minimum allowed length of a master key, in bytes.
	MinMasterKeyLength = 16

	// DefaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
	DefaultKeyDerivationAlgorithm = "scrypt-65536-8-1"
)

// SupportedKeyDerivationAlgorithms lists supported key derivation algorithms.
var SupportedKeyDerivationAlgorithms = []string{
	"scrypt-65536-8-1",
	"pbkdf2-sha256-100000",
}

type passwordCredentials struct {
	password string
}

func (pc *passwordCredentials) GetMasterKey(f Options) ([]byte, error) {
	switch f.KeyDerivationAlgorithm {
	case "pbkdf2-sha256-100000":
		return pbkdf2.Key([]byte(pc.password), f.UniqueID, 100000, passwordBasedKeySize, sha256.New), nil

	case "scrypt-65536-8-1":
		return scrypt.Key([]byte(pc.password), f.UniqueID, 65536, 8, 1, passwordBasedKeySize)

	default:
		return nil, fmt.Errorf("unsupported key algorithm: %v", f.KeyDerivationAlgorithm)
	}
}

// Password returns password-based Credentials with the specified password.
func Password(password string) (Credentials, error) {
	if len(password) < MinPasswordLength {
		return nil, fmt.Errorf("password too short")
	}

	return &passwordCredentials{password}, nil
}
