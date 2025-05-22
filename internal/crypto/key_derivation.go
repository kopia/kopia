package crypto

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"

	"github.com/kopia/kopia/internal/impossible"
)

// DeriveKeyFromMasterKey computes a key for a specific purpose and length using HKDF based on the master key.
func DeriveKeyFromMasterKey(masterKey, salt, purpose []byte, length int) []byte {
	if len(masterKey) == 0 {
		panic("invalid master key")
	}

	key := make([]byte, length)
	k := hkdf.New(sha256.New, masterKey, salt, purpose)

	_, err := io.ReadFull(k, key)

	impossible.PanicOnError(err)

	return key
}
