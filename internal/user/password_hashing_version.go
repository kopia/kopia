package user

import "github.com/pkg/errors"

// defaultPasswordHashVersion is the default scheme used for user password hashing.
const defaultPasswordHashVersion = ScryptHashVersion

// getPasswordHashAlgorithm returns the password hash algorithm given a version.
func getPasswordHashAlgorithm(passwordHashVersion int) (string, error) {
	switch passwordHashVersion {
	case ScryptHashVersion:
		return scryptHashAlgorithm, nil
	case Pbkdf2HashVersion:
		return pbkdf2HashAlgorithm, nil
	default:
		return "", errors.Errorf("unsupported hash version (%d)", passwordHashVersion)
	}
}
