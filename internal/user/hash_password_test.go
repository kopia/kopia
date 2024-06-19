package user

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/stretchr/testify/require"
)

func TestHashPassword_encoding(t *testing.T) {
	bogusPassword := petname.Generate(2, "+")

	h, err := HashPassword(bogusPassword)
	require.NoError(t, err)
	require.NotEmpty(t, h)

	// test encoding implementation details
	j, err := base64.StdEncoding.DecodeString(h)

	require.NoError(t, err)
	require.NotEmpty(t, j)

	var ph passwordHash

	err = json.Unmarshal(j, &ph)

	require.NoError(t, err)
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
