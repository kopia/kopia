package user_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/user"
)

func TestUserProfile(t *testing.T) {
	p := &user.Profile{
		PasswordHashVersion: user.ScryptHashVersion,
	}

	isValid := p.IsValidPassword("bar")

	require.False(t, isValid, "password unexpectedly valid!")

	p.SetPassword("foo")

	isValid = p.IsValidPassword("foo")

	require.True(t, isValid, "password not valid!")

	isValid = p.IsValidPassword("bar")

	require.False(t, isValid, "password unexpectedly valid!")

	// Different key derivation algorithm besides the original should fail
	p.PasswordHashVersion = user.Pbkdf2HashVersion
	isValid = p.IsValidPassword("foo")

	require.False(t, isValid, "password unexpectedly valid!")
}

func TestBadPasswordHashVersion(t *testing.T) {
	// mock a valid password
	p := &user.Profile{
		PasswordHashVersion: user.ScryptHashVersion,
	}

	p.SetPassword("foo")
	// Assume the key derivation algorithm is bad. This will cause
	// a panic when validating
	p.PasswordHashVersion = 0

	isValid := p.IsValidPassword("foo")

	require.False(t, isValid, "password unexpectedly valid!")
}

func TestNilUserProfile(t *testing.T) {
	var p *user.Profile

	isValid := p.IsValidPassword("bar")

	require.False(t, isValid, "password unexpectedly valid!")
}

func TestInvalidPasswordHash(t *testing.T) {
	cases := [][]byte{
		[]byte("**invalid*base64*"),
		[]byte(""),
	}

	for _, tc := range cases {
		p := &user.Profile{
			PasswordHash:        tc,
			PasswordHashVersion: 1,
		}
		isValid := p.IsValidPassword("some-password")

		require.False(t, isValid, "password unexpectedly valid for %v", tc)
	}
}
