// Package gcs implements Storage based on Google Cloud Storage bucket.
package gcs

import (
	"context"
	"io/fs"
	"os"
	"testing"

	gcsclient "cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
)

func TestGCSStorageCredentialsHelpers(t *testing.T) {
	ctx := context.Background()
	scope := gcsclient.ScopeReadOnly

	var fileMode fs.FileMode = 0o644

	// Service Account key
	gsaKeyServiceAccount := `{
		"type": "service_account",
		"project_id": "kopia-test-project",
		"private_key_id": "kopia-test",
		"private_key": "some-private-key",
		"client_email": "kopia-test@developer.gserviceaccount.com",
		"client_id": "kopia-test.apps.googleusercontent.com",
		"auth_uri": "https://accounts.google.com/o/oauth2/auth",
		"token_uri": "http://localhost:8080/token"
	  }`
	gsaKeyServiceAccountFileName := "service-account.json"
	errWriteFile := os.WriteFile(gsaKeyServiceAccountFileName, []byte(gsaKeyServiceAccount), fileMode)
	require.NoError(t, errWriteFile)
	t.Cleanup(func() {
		os.Remove(gsaKeyServiceAccountFileName)
	})

	t.Run("tokenSourceFromCredentialsJSON with service account key", func(t *testing.T) {
		ts, err := tokenSourceFromCredentialsJSON(ctx, []byte(gsaKeyServiceAccount), scope)
		require.NoError(t, err)
		require.NotNil(t, ts)
	})
	t.Run("tokenSourceFromCredentialsFile with service account key file", func(t *testing.T) {
		ts, err := tokenSourceFromCredentialsFile(ctx, gsaKeyServiceAccountFileName, scope)
		require.NoError(t, err)
		require.NotNil(t, ts)
	})

	// External Account key
	gsaKeyExternalAccount := `{
		"type": "external_account",
		"audience": "some-audience",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url": "https://sts.googleapis.com/v1/token",
		"service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/kopia-test@kopia-test-project.iam.gserviceaccount.com:generateAccessToken",
		"credential_source": {
		  "file": "/var/run/secrets/serviceaccount/token",
		  "format": {
			"type": "text"
		  }
		}
	  }`
	gsaKeyExternalAccountFileName := "external-account.json"
	errWriteFile = os.WriteFile(gsaKeyExternalAccountFileName, []byte(gsaKeyExternalAccount), fileMode)
	require.NoError(t, errWriteFile)
	t.Cleanup(func() {
		os.Remove(gsaKeyExternalAccountFileName)
	})

	t.Run("tokenSourceFromCredentialsJSON with external account key", func(t *testing.T) {
		ts, err := tokenSourceFromCredentialsJSON(ctx, []byte(gsaKeyExternalAccount), scope)
		require.NoError(t, err)
		require.NotNil(t, ts)
	})
	t.Run("tokenSourceFromCredentialsFile with external account key file", func(t *testing.T) {
		ts, err := tokenSourceFromCredentialsFile(ctx, gsaKeyExternalAccountFileName, scope)
		require.NoError(t, err)
		require.NotNil(t, ts)
	})
}
