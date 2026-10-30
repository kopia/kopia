package crypto

import (
	"crypto/hkdf"
	"crypto/sha256"

	"github.com/pkg/errors"
)

var errInvalidMasterKey = errors.New("invalid primary key")

// DeriveKeyFromMasterKey computes a key for a specific purpose and length using HKDF based on the master key.
func DeriveKeyFromMasterKey(masterKey, salt []byte, purpose string, length int) (derivedKey []byte, err error) {
	if len(masterKey) == 0 {
		return nil, errors.Wrap(errInvalidMasterKey, "empty key")
	}

	if derivedKey, err = hkdf.Key(sha256.New, masterKey, salt, purpose, length); err != nil {
		return nil, errors.Wrap(err, "unable to derive key")
	}

	return derivedKey, nil
}
