package auth

import (
	"errors"

	"golang.org/x/crypto/curve25519"
)

const (
	pbkdf2Rounds  = 10000
	masterKeySize = 32
)

type UserPrivateKey struct {
	key [32]byte
}

func (prv UserPrivateKey) Bytes() []byte {
	r := make([]byte, 32)
	copy(r, prv.key[:])
	return r
}

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

type UserPublicKey struct {
	key [32]byte
}
