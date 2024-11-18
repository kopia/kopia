package user

import "github.com/pkg/errors"

// getPasswordHashAlgorithm returns the password hash algorithm given a version.
func getPasswordHashAlgorithm(passwordHashVersion int) (string, error) {
	switch passwordHashVersion {
	// when the version is unsetDefaulHashVersion, map it to ScryptHashVersion
	case unsetDefaulHashVersion, ScryptHashVersion:
		return scryptHashAlgorithm, nil
	case Pbkdf2HashVersion:
		return pbkdf2HashAlgorithm, nil
	default:
		return "", errors.Errorf("unsupported hash version (%d)", passwordHashVersion)
	}
}
