package crypto

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"
)

// DeriveKeyFromMasterKey computes a key for a specific purpose and length using HKDF based on the master key.
func DeriveKeyFromMasterKey(masterKey, salt, purpose []byte, length int) []byte {
	if len(masterKey) == 0 {
		panic("invalid master key")
	}

	key := make([]byte, length)
	k := hkdf.New(sha256.New, masterKey, salt, purpose)

	if _, err := io.ReadFull(k, key); err != nil {
		panic("unable to derive key from master key, this should never happen")
	}

	return key
}
