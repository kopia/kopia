package repo

import (
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/scrypt"
)

// defaultKeyDerivationAlgorithm is the key derivation algorithm for new configurations.
const defaultKeyDerivationAlgorithm = "scrypt-65536-8-1"

func (f formatBlock) deriveMasterKeyFromPassword(password string) ([]byte, error) {
	const masterKeySize = 32

	switch f.KeyDerivationAlgorithm {
	case "scrypt-65536-8-1":
		return scrypt.Key([]byte(password), f.UniqueID, 65536, 8, 1, masterKeySize)

	default:
		return nil, fmt.Errorf("unsupported key algorithm: %v", f.KeyDerivationAlgorithm)
	}
}

// deriveKeyFromMasterKey computes a key for a specific purpose and length using HKDF based on the master key.
func deriveKeyFromMasterKey(masterKey, uniqueID, purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, masterKey, uniqueID, purpose)
	io.ReadFull(k, key) //nolint:errcheck
	return key
}
