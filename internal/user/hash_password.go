package user

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"

	"github.com/pkg/errors"
)

type passwordHash struct {
	PasswordHashVersion int    `json:"passwordHashVersion"`
	PasswordHash        []byte `json:"passwordHash"`
}

// HashPassword computes the hash for the given password and an encoded hash
// that can be passed to Profile.SetPasswordHash().
func HashPassword(password string) (string, error) {
	const hashVersion = defaultPasswordHashVersion

	salt := make([]byte, passwordHashSaltLength)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return "", errors.Wrap(err, "error generating salt")
	}

	h, err := computePasswordHash(password, salt, hashVersion)
	if err != nil {
		return "", errors.Wrap(err, "error hashing password")
	}

	pwh := passwordHash{
		PasswordHashVersion: hashVersion,
		PasswordHash:        h,
	}

	j, err := json.Marshal(pwh)
	if err != nil {
		return "", errors.Wrap(err, "error encoding password hash")
	}

	return base64.StdEncoding.EncodeToString(j), nil
}
