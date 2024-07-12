package user

import (
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
)

//nolint:gochecknoglobals
var dummyHashThatNeverMatchesAnyPassword = initDummyHash()

func initDummyHash() []byte {
	s := make([]byte, passwordHashSaltLength+passwordHashLength)

	for i := range s {
		s[i] = 0xFF
	}

	return s
}

func (p *Profile) setPassword(password string) error {
	salt := make([]byte, passwordHashSaltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return errors.Wrap(err, "error generating salt")
	}

	var err error

	p.PasswordHash, err = computePasswordHash(password, salt, p.PasswordHashVersion)

	return err
}

func computePasswordHash(password string, salt []byte, passwordHashVersion int) ([]byte, error) {
	hashingAlgo, err := getPasswordHashAlgorithm(passwordHashVersion)
	if err != nil {
		return nil, err
	}

	key, err := crypto.DeriveKeyFromPassword(password, salt, passwordHashLength, hashingAlgo)
	if err != nil {
		return nil, errors.Wrap(err, "error hashing password")
	}

	payload := append(append([]byte(nil), salt...), key...)

	return payload, nil
}

func isValidPassword(password string, hashedPassword []byte, passwordHashVersion int) (bool, error) {
	if len(hashedPassword) != passwordHashSaltLength+passwordHashLength {
		return false, nil
	}

	salt := hashedPassword[0:passwordHashSaltLength]

	h, err := computePasswordHash(password, salt, passwordHashVersion)
	if err != nil {
		return false, err
	}

	return subtle.ConstantTimeCompare(h, hashedPassword) != 0, nil
}
