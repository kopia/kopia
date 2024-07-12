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

func decodeHashedPassword(encodedHash string) (*passwordHash, error) {
	var h passwordHash

	passwordHashJSON, err := base64.StdEncoding.DecodeString(encodedHash)
	if err != nil {
		return nil, errors.Wrap(err, "decoding password hash")
	}

	if err := json.Unmarshal(passwordHashJSON, &h); err != nil {
		return nil, errors.Wrap(err, "unmarshalling password hash")
	}

	return &h, nil
}

// validates hashing algorithm and password hash length.
func (h *passwordHash) validate() error {
	if _, err := getPasswordHashAlgorithm(h.PasswordHashVersion); err != nil {
		return errors.Wrap(err, "invalid password hash version")
	}

	if len(h.PasswordHash) != passwordHashSaltLength+passwordHashLength {
		return errors.Errorf("invalid hash length: %v", len(h.PasswordHash))
	}

	return nil
}
