package vault

import (
	"errors"

	"golang.org/x/crypto/curve25519"
)

const (
	pbkdf2Rounds  = 10000
	masterKeySize = 32
)

// UserPrivateKey encapsulates secret key belonging to a user.
type UserPrivateKey struct {
	key [32]byte
}

// UserPublicKey encapsulates public key belonging to a user.
type UserPublicKey struct {
	key [32]byte
}

// Bytes returns the private key bytes.
func (prv UserPrivateKey) Bytes() []byte {
	r := make([]byte, 32)
	copy(r, prv.key[:])
	return r
}

// PublicKey returns public key associated with the private key.
func (prv UserPrivateKey) PublicKey() *UserPublicKey {
	pub := &UserPublicKey{}

	curve25519.ScalarBaseMult(&pub.key, &prv.key)
	return pub
}

func newPrivateKey(key []byte) (*UserPrivateKey, error) {
	if len(key) != 32 {
		return nil, errors.New("unsupported key length")
	}

	k := &UserPrivateKey{}
	copy(k.key[:], key)
	return k, nil
}
