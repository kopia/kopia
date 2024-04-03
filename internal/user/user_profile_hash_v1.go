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
	// Legacy hash version system translates to KeyDerivationAlgorithm
	hashVersion1 = 1 // this translates to Scrypt KeyDerivationAlgorithm

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

	// Setup to handle legacy hashVersion.
	if p.PasswordHashVersion == hashVersion1 {
		p.KeyDerivationAlgorithm = crypto.ScryptAlgorithm
	}
	p.PasswordHash, err = computePasswordHashV1(password, salt, p.KeyDerivationAlgorithm)

	return err
}

func computePasswordHashV1(password string, salt []byte, keyDerivationAlgorithm string) ([]byte, error) {
	key, err := crypto.DeriveKeyFromPassword(password, salt, keyDerivationAlgorithm)
	if err != nil {
		return nil, errors.Wrap(err, "error deriving key from password")
	}

	payload := append(append([]byte(nil), salt...), key...)

	return payload, nil
}

func isValidPasswordV1(password string, hashedPassword []byte, keyDerivationAlgorithm string) bool {
	if len(hashedPassword) != v1SaltLength+crypto.MasterKeyLength {
		return false
	}

	salt := hashedPassword[0:v1SaltLength]

	h, err := computePasswordHashV1(password, salt, keyDerivationAlgorithm)
	if err != nil {
		panic(err)
	}

	return subtle.ConstantTimeCompare(h, hashedPassword) != 0
}
