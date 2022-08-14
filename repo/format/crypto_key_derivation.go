package format

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"

	"github.com/kopia/kopia/internal/impossible"
)

// DeriveKeyFromMasterKey computes a key for a specific purpose and length using HKDF based on the master key.
func DeriveKeyFromMasterKey(masterKey, uniqueID, purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, masterKey, uniqueID, purpose)

	_, err := io.ReadFull(k, key)
	impossible.PanicOnError(err)

	return key
}
