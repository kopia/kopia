package auth

import (
	"crypto/sha256"
	"io"

	"golang.org/x/crypto/hkdf"
)

// KeyManager manages key derivation from a master key.
type KeyManager struct {
	masterKey []byte
	uniqueID  []byte
}

// DeriveKey computes a key for a specific purpose and length using HKDF based on the master key.
func (km *KeyManager) DeriveKey(purpose []byte, length int) []byte {
	key := make([]byte, length)
	k := hkdf.New(sha256.New, km.masterKey, km.uniqueID, purpose)
	io.ReadFull(k, key)
	return key
}

// NewKeyManager constructs a KeyManager with given credentials and options.
func NewKeyManager(creds Credentials, so SecurityOptions) (*KeyManager, error) {
	mk, err := creds.GetMasterKey(so)
	if err != nil {
		return nil, err
	}

	return &KeyManager{mk, so.UniqueID}, nil
}
