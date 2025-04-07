package user

import (
	"github.com/kopia/kopia/repo/manifest"
)

const (
	// default password hash version when it is not explicitly set in the user
	// profile, this always maps to ScryptHashVersion.
	unsetDefaulHashVersion = 0

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

// SetPasswordHash decodes and validates encodedhash, if it is a valid hash
// then it sets it as the password hash for the user profile.
func (p *Profile) SetPasswordHash(encodedHash string) error {
	ph, err := decodeHashedPassword(encodedHash)
	if err != nil {
		return err
	}

	if err := ph.validate(); err != nil {
		return err
	}

	p.PasswordHashVersion = ph.PasswordHashVersion
	p.PasswordHash = ph.PasswordHash

	return nil
}

// IsValidPassword determines whether the password is valid for a given user.
func (p *Profile) IsValidPassword(password string) (bool, error) {
	if p == nil {
		// return false when the user profile does not exist,
		// but use the same amount of time as when checking the password for a
		// valid user to avoid revealing whether the account exists.
		_, err := isValidPassword(password, dummyHashThatNeverMatchesAnyPassword, defaultPasswordHashVersion)

		return false, err
	}

	return isValidPassword(password, p.PasswordHash, p.PasswordHashVersion)
}
