package user

import (
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

// parameters for v1 hashing.
const (
	hashVersion1 = 1

	v1ScryptN    = 65536
	v1ScryptR    = 8
	v1ScryptP    = 1
	v1SaltLength = 32
	v1KeyLength  = 32
)

//nolint:gochecknoglobals
var dummyV1HashThatNeverMatchesAnyPassword = make([]byte, v1KeyLength+v1SaltLength)

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
	key, err := scrypt.Key([]byte(password), salt, v1ScryptN, v1ScryptR, v1ScryptP, v1KeyLength)
	if err != nil {
		panic("unexpected scrypt error")
	}

	payload := append(append([]byte(nil), salt...), key...)

	return payload
}

func isValidPasswordV1(password string, hashedPassword []byte) bool {
	if len(hashedPassword) != v1SaltLength+v1KeyLength {
		return false
	}

	salt := hashedPassword[0:v1SaltLength]

	h := computePasswordHashV1(password, salt)

	return subtle.ConstantTimeCompare(h, hashedPassword) != 0
}
