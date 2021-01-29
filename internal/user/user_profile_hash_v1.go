package user

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/crypto/scrypt"
)

//  parameters for v1 hashing.
const (
	v1Prefix = "v1:"

	v1ScryptN    = 65536
	v1ScryptR    = 8
	v1ScryptP    = 1
	v1SaltLength = 32
	v1KeyLength  = 32
)

func (p *Profile) setPasswordV1(password string) error {
	salt := make([]byte, v1SaltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return errors.Wrap(err, "error generating salt")
	}

	p.PasswordHash = computePasswordHashV1(password, salt)

	return nil
}

func computePasswordHashV1(password string, salt []byte) string {
	key, err := scrypt.Key([]byte(password), salt, v1ScryptN, v1ScryptR, v1ScryptP, v1KeyLength)
	if err != nil {
		panic("unexpected scrypt error")
	}

	payload := append(append([]byte(nil), salt...), key...)

	return v1Prefix + base64.RawURLEncoding.EncodeToString(payload)
}

func isValidPasswordV1(password, hashedPassword string) bool {
	if len(hashedPassword) < len(v1Prefix) {
		return false
	}

	data, err := base64.RawURLEncoding.DecodeString(hashedPassword[len(v1Prefix):])
	if err != nil {
		return false
	}

	if len(data) != v1SaltLength+v1KeyLength {
		return false
	}

	salt := data[0:v1SaltLength]

	h := computePasswordHashV1(password, salt)

	return subtle.ConstantTimeCompare([]byte(h), []byte(hashedPassword)) != 0
}
