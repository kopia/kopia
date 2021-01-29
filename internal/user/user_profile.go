package user

import (
	"strings"

	"github.com/kopia/kopia/repo/manifest"
)

// Profile describes information about a single user.
type Profile struct {
	ManifestID manifest.ID `json:"-"`

	Username     string `json:"username"`
	PasswordHash string `json:"passwordHash"`
}

// SetPassword changes the password for a user profile.
func (p *Profile) SetPassword(password string) error {
	return p.setPasswordV1(password)
}

// IsValidPassword determines whether the password is valid for a given user.
func (p *Profile) IsValidPassword(password string) bool {
	switch {
	case strings.HasPrefix(p.PasswordHash, v1Prefix):
		return isValidPasswordV1(password, p.PasswordHash)

	default:
		return false
	}
}
