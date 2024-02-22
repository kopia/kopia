package user

import (
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
)

// parameters for v1 hashing.
const (
	hashVersion1 = 1

	v1SaltLength = 32
)

//nolint:gochecknoglobals
var dummyV1HashThatNeverMatchesAnyPassword = make([]byte, crypto.MasterKeyLength+v1SaltLength)

func (p *Profile) setPasswordV1(password string) error {
	salt := make([]byte, v1SaltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return errors.Wrap(err, "error generating salt")
	}

	var err error
	p.PasswordHashVersion = 1
	p.PasswordHash, err = computePasswordHashV1(password, salt)

	return err
}

func computePasswordHashV1(password string, salt []byte) ([]byte, error) {
	key, err := crypto.DeriveKeyFromPassword(password, salt, crypto.DefaultKeyDerivationAlgorithm)
	if err != nil {
		return nil, err
	}

	payload := append(append([]byte(nil), salt...), key...)

	return payload, nil
}

func isValidPasswordV1(password string, hashedPassword []byte) bool {
	if len(hashedPassword) != v1SaltLength+crypto.MasterKeyLength {
		return false
	}

	salt := hashedPassword[0:v1SaltLength]

	h, err := computePasswordHashV1(password, salt)
	if err != nil {
		return false
	}

	return subtle.ConstantTimeCompare(h, hashedPassword) != 0
}
