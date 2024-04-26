package user

import (
	"math/rand"

	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/repo/manifest"

	"github.com/pkg/errors"
)

const (
	// ScryptHashVersion is the version representation of the scrypt algorithm.
	ScryptHashVersion = 1
	// ScryptHashAlgorithm is the scrypt password hashing algorithm. This must match crypto.ScryptAlgorithm.
	ScryptHashAlgorithm = "scrypt-65536-8-1"

	// Pbkdf2HashVersion is the version representation of the pbkdf2 algorithm.
	Pbkdf2HashVersion = 2
	// Pbkdf2HashAlgorithm is the pbkdf2 password hashing algorithm. This must match crypto.Pbkdf2Algorithm.
	Pbkdf2HashAlgorithm = "pbkdf2-sha256-600000"
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
func (p *Profile) IsValidPassword(password string) bool {
	var invalidProfile bool

	var passwordHashAlgorithm string

	var err error

	if p == nil {
		invalidProfile = true
	} else {
		passwordHashAlgorithm, err = GetPasswordHashAlgorithm(p.PasswordHashVersion)
		if err != nil {
			invalidProfile = true
		}
	}

	if invalidProfile {
		algorithms := crypto.AllowedKeyDerivationAlgorithms()
		// if the user profile is invalid, either a non-existing user name or password
		// hash version, then return false but use the same amount of time as when we
		// compare against valid user to avoid revealing whether the user account exists.
		isValidPassword(password, dummyV1HashThatNeverMatchesAnyPassword, algorithms[rand.Intn(len(algorithms))]) //nolint:gosec

		return false
	}

	return isValidPassword(password, p.PasswordHash, passwordHashAlgorithm)
}

// GetPasswordHashAlgorithm returns the password hash algorithm given a version.
func GetPasswordHashAlgorithm(passwordHashVersion int) (string, error) {
	switch passwordHashVersion {
	case ScryptHashVersion:
		return ScryptHashAlgorithm, nil
	case Pbkdf2HashVersion:
		return Pbkdf2HashAlgorithm, nil
	default:
		return "", errors.Errorf("unsupported hash version (%d)", passwordHashVersion)
	}
}

// GetPasswordHashVersion returns the password hash version given an algorithm.
func GetPasswordHashVersion(passwordHashAlgorithm string) (int, error) {
	switch passwordHashAlgorithm {
	case ScryptHashAlgorithm:
		return ScryptHashVersion, nil
	case Pbkdf2HashAlgorithm:
		return Pbkdf2HashVersion, nil
	default:
		return 0, errors.Errorf("unsupported hash algorithm (%s)", passwordHashAlgorithm)
	}
}
