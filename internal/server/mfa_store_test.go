package server

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMFAMasterKeySurvivesSigningKeyChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mfa.json")

	store1, err := newMFACredentialStore(path, []byte("signing-key-one"))
	require.NoError(t, err)

	enc, err := store1.encryptSecret("totp-secret-value")
	require.NoError(t, err)
	require.NoError(t, store1.update("kopia", func(u *mfaUserCredentials) error {
		u.TOTPEnabled = true
		u.TOTPSecretEnc = enc

		return nil
	}))

	store2, err := newMFACredentialStore(path, []byte("completely-different-signing-key"))
	require.NoError(t, err)

	secret, ok := store2.totpSecret("kopia")
	require.True(t, ok)
	require.Equal(t, "totp-secret-value", secret)
}
