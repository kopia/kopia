package user

import (
	"strconv"
	"testing"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/stretchr/testify/require"
)

func TestHashPassword_encoding(t *testing.T) {
	bogusPassword := petname.Generate(2, "+")

	h, err := HashPassword(bogusPassword)
	require.NoError(t, err)
	require.NotEmpty(t, h)

	// roundtrip
	ph, err := decodeHashedPassword(h)

	require.NoError(t, err)
	require.NotEmpty(t, ph)
	require.NotZero(t, ph.PasswordHashVersion)
	require.NotEmpty(t, ph.PasswordHash)

	p := Profile{
		PasswordHashVersion: ph.PasswordHashVersion,
		PasswordHash:        ph.PasswordHash,
	}

	valid, err := p.IsValidPassword(bogusPassword)

	require.NoError(t, err)
	require.True(t, valid)
}

func TestPasswordHashValidate(t *testing.T) {
	cases := []struct {
		ph          passwordHash
		expectError bool
	}{
		{
			expectError: true,
		},
		{
			ph: passwordHash{
				PasswordHashVersion: -3,
			},
			expectError: true,
		},
		{
			ph: passwordHash{
				PasswordHashVersion: defaultPasswordHashVersion,
				// empty PasswordHash
			},
			expectError: true,
		},
		{
			ph: passwordHash{
				PasswordHashVersion: defaultPasswordHashVersion,
				// PasswordHash with invalid length
				PasswordHash: []byte{'z', 'a'},
			},
			expectError: true,
		},
		{
			ph: passwordHash{
				PasswordHashVersion: defaultPasswordHashVersion,
				PasswordHash:        make([]byte, passwordHashSaltLength+passwordHashLength),
			},
			expectError: false,
		},
	}

	for i, tc := range cases {
		t.Run("i_"+strconv.Itoa(i), func(t *testing.T) {
			gotErr := tc.ph.validate()
			if tc.expectError {
				require.Error(t, gotErr)
			} else {
				require.NoError(t, gotErr)
			}
		})
	}
}
