package user

import (
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/manifest"
)

const (
	// ScryptHashVersion is the version representation of the scrypt algorithm.
	ScryptHashVersion = 1
	// scryptHashAlgorithm is the scrypt password hashing algorithm. This must match crypto.ScryptAlgorithm.
	scryptHashAlgorithm = "scrypt-65536-8-1"

	// Pbkdf2HashVersion is the version representation of the pbkdf2 algorithm.
	Pbkdf2HashVersion = 2
	// pbkdf2HashAlgorithm is the pbkdf2 password hashing algorithm. This must match crypto.Pbkdf2Algorithm.
	pbkdf2HashAlgorithm = "pbkdf2-sha256-600000"

	passwordHashLength     = 32
	passwordHashSaltLength = 32
)

// Profile describes information about a single user.
type Profile struct {
	ManifestID manifest.ID `json:"-"`

	Username            string `json:"username"`
	PasswordHashVersion int    `json:"passwordHashVersion,omitempty"`
	PasswordHash        []byte `json:"passwordHash"`
}

// SetPassword changes the password for a user profile.
func (p *Profile) SetPassword(password string) error {
	return p.setPassword(password)
}

// IsValidPassword determines whether the password is valid for a given user.
func (p *Profile) IsValidPassword(password string) (bool, error) {
	if p == nil {
		// if the user profile is invalid, either a non-existing user name, then
		// return false but use the same amount of time as when we
		// compare against valid user to avoid revealing whether the user account exists.
		_, err := isValidPassword(password, dummyHashThatNeverMatchesAnyPassword, defaultPasswordHashVersion)

		return false, err
	}

	return isValidPassword(password, p.PasswordHash, p.PasswordHashVersion)
}

// GetPasswordHashVersion returns the password hash version given an algorithm.
func GetPasswordHashVersion(passwordHashAlgorithm string) (int, error) {
	switch passwordHashAlgorithm {
	case scryptHashAlgorithm:
		return ScryptHashVersion, nil
	case pbkdf2HashAlgorithm:
		return Pbkdf2HashVersion, nil
	default:
		return 0, errors.Errorf("unsupported hash algorithm (%s)", passwordHashAlgorithm)
	}
}
