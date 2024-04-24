package user

import (
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
)

//nolint:gochecknoglobals
var dummyV1HashThatNeverMatchesAnyPassword = make([]byte, crypto.MasterKeyLength+crypto.V1SaltLength)

func (p *Profile) setPassword(password string) error {
	passwordHashAlgorithm, err := GetPasswordHashAlgorithm(p.PasswordHashVersion)
	if err != nil {
		return err
	}

	saltLength, err := crypto.RecommendedSaltLength(passwordHashAlgorithm)
	if err != nil {
		return errors.Wrap(err, "error getting recommended salt length")
	}

	salt := make([]byte, saltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return errors.Wrap(err, "error generating salt")
	}

	p.PasswordHash, err = computePasswordHash(password, salt, passwordHashAlgorithm)

	return err
}

func computePasswordHash(password string, salt []byte, keyDerivationAlgorithm string) ([]byte, error) {
	key, err := crypto.DeriveKeyFromPassword(password, salt, keyDerivationAlgorithm)
	if err != nil {
		return nil, errors.Wrap(err, "error deriving key from password")
	}

	payload := append(append([]byte(nil), salt...), key...)

	return payload, nil
}

func isValidPassword(password string, hashedPassword []byte, keyDerivationAlgorithm string) bool {
	saltLength, err := crypto.RecommendedSaltLength(keyDerivationAlgorithm)
	if err != nil {
		panic(err)
	}

	if len(hashedPassword) != saltLength+crypto.MasterKeyLength {
		return false
	}

	salt := hashedPassword[0:saltLength]

	h, err := computePasswordHash(password, salt, keyDerivationAlgorithm)
	if err != nil {
		panic(err)
	}

	return subtle.ConstantTimeCompare(h, hashedPassword) != 0
}
