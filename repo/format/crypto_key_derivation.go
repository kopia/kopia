package format

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"
)

// DeriveKeyFromMasterKey computes a key for a specific purpose and length using HKDF based on the master key.
func DeriveKeyFromMasterKey(masterKey, uniqueID, purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, masterKey, uniqueID, purpose)

	if _, err := io.ReadFull(k, key); err != nil {
		panic("unable to derive key from master key, this should never happen")
	}

	return key
}
