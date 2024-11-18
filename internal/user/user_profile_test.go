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

	isValid, err := p.IsValidPassword("bar")

	require.False(t, isValid, "password unexpectedly valid!")
	require.NoError(t, err)

	p.SetPassword("foo")

	isValid, err = p.IsValidPassword("foo")

	require.True(t, isValid, "password not valid!")
	require.NoError(t, err)

	isValid, err = p.IsValidPassword("bar")

	require.False(t, isValid, "password unexpectedly valid!")
	require.NoError(t, err)
}

func TestBadPasswordHashVersionWithSCrypt(t *testing.T) {
	// mock a valid password
	p := &user.Profile{
		PasswordHashVersion: user.ScryptHashVersion,
	}

	p.SetPassword("foo")

	isValid, err := p.IsValidPassword("foo")

	require.True(t, isValid, "password not valid!")
	require.NoError(t, err)

	// A password hashing algorithm different from the original should fail
	p.PasswordHashVersion = user.Pbkdf2HashVersion
	isValid, err = p.IsValidPassword("foo")

	require.False(t, isValid, "password unexpectedly valid!")
	require.NoError(t, err)
}

func TestBadPasswordHashVersionWithPbkdf2(t *testing.T) {
	const dummyTestPassword = "foo"

	p := &user.Profile{
		PasswordHashVersion: user.Pbkdf2HashVersion,
	}

	p.SetPassword(dummyTestPassword)

	isValid, err := p.IsValidPassword(dummyTestPassword)

	require.True(t, isValid, "password not valid!")
	require.NoError(t, err)

	// A password hashing algorithm different from the original should fail
	p.PasswordHashVersion = user.ScryptHashVersion
	isValid, err = p.IsValidPassword(dummyTestPassword)

	require.False(t, isValid, "password unexpectedly valid!")
	require.NoError(t, err)

	p.PasswordHashVersion = 0
	isValid, err = p.IsValidPassword(dummyTestPassword)

	require.False(t, isValid, "password unexpectedly valid!")
	require.NoError(t, err)
}

func TestUnsetPasswordHashVersion(t *testing.T) {
	const dummyTestPassword = "foo"

	p := &user.Profile{
		PasswordHashVersion: user.ScryptHashVersion,
	}

	p.SetPassword(dummyTestPassword)

	isValid, err := p.IsValidPassword(dummyTestPassword)

	require.True(t, isValid, "password not valid!")
	require.NoError(t, err)

	// Unset password hashing algorithm
	p.PasswordHashVersion = 0

	isValid, err = p.IsValidPassword(dummyTestPassword)

	require.True(t, isValid, "password unexpectedly invalid!")
	require.NoError(t, err)
}

func TestNilUserProfile(t *testing.T) {
	var p *user.Profile

	isValid, err := p.IsValidPassword("bar")

	require.False(t, isValid, "password unexpectedly valid!")
	require.NoError(t, err)
}

func TestInvalidPasswordHash(t *testing.T) {
	cases := [][]byte{
		[]byte("**invalid*base64*"),
		[]byte(""),
	}

	for _, tc := range cases {
		p := &user.Profile{
			PasswordHash:        tc,
			PasswordHashVersion: user.ScryptHashVersion,
		}
		isValid, err := p.IsValidPassword("some-password")

		require.False(t, isValid, "password unexpectedly valid for %v", tc)
		require.NoError(t, err)
	}
}
