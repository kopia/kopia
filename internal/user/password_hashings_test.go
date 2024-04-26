package user

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/crypto"
)

// The password hashing constants defined in this package are used as "lookup
// keys" for the register password-based key derivers in the crypto package.
// This trivial test is a change detector to ensure that the constants defined
// in the user package match those defined in the crypto package.
func TestPasswordHashingConstantMatchCryptoPackage(t *testing.T) {
	require.Equal(t, crypto.ScryptAlgorithm, scryptHashAlgorithm)
	require.Equal(t, crypto.Pbkdf2Algorithm, pbkdf2HashAlgorithm)
}
