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
	var fileMode fs.FileMode = 0644

	// test tokenSourceFromCredentialsJSON with service account key
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

	ts, err := tokenSourceFromCredentialsJSON(ctx, []byte(gsaKeyServiceAccount), scope)
	require.NoError(t, err)
	require.NotNil(t, ts)
	// test tokenSourceFromCredentialsFile with service account key
	gsaKeyServiceAccountFileName := "service-acount.json"
	err = os.WriteFile(gsaKeyServiceAccountFileName, []byte(gsaKeyServiceAccount), fileMode)
	require.NoError(t, err)
	defer os.Remove(gsaKeyServiceAccountFileName)
	ts, err = tokenSourceFromCredentialsFile(ctx, gsaKeyServiceAccountFileName, scope)
	require.NoError(t, err)
	require.NotNil(t, ts)

	// test tokenSourceFromCredentialsJSON with external account key
	gsaKeyExternalAccount := `{
		"type": "external_account",
		"audience": "some-audience",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url": "https://sts.googleapis.com/v1/token",
		"service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/kopia-test@kopia-test-project.iam.gserviceaccount.com:generateAccessToken",
		"credential_source": {
		  "file": "/var/run/secrets/openshift/serviceaccount/token",
		  "format": {
			"type": "text"
		  }
		}
	  }`
	ts, err = tokenSourceFromCredentialsJSON(ctx, []byte(gsaKeyExternalAccount), scope)
	require.NoError(t, err)
	require.NotNil(t, ts)
	// test tokenSourceFromCredentialsFile with external account key
	gsaKeyExternalAccountFileName := "external-acount.json"
	err = os.WriteFile(gsaKeyExternalAccountFileName, []byte(gsaKeyExternalAccount), fileMode)
	require.NoError(t, err)
	defer os.Remove(gsaKeyExternalAccountFileName)
	ts, err = tokenSourceFromCredentialsFile(ctx, gsaKeyExternalAccountFileName, scope)
	require.NoError(t, err)
	require.NotNil(t, ts)

}
