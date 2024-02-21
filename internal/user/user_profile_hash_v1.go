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

	p.PasswordHashVersion = 1
	p.PasswordHash = computePasswordHashV1(password, salt)

	return nil
}

func computePasswordHashV1(password string, salt []byte) []byte {
	key, err := crypto.DeriveKeyFromPassword(password, salt, crypto.DefaultKeyDerivationAlgorithm)
	if err != nil {
		panic("unexpected key derivation error")
	}

	payload := append(append([]byte(nil), salt...), key...)

	return payload
}

func isValidPasswordV1(password string, hashedPassword []byte) bool {
	if len(hashedPassword) != v1SaltLength+crypto.MasterKeyLength {
		return false
	}

	salt := hashedPassword[0:v1SaltLength]

	h := computePasswordHashV1(password, salt)

	return subtle.ConstantTimeCompare(h, hashedPassword) != 0
}
