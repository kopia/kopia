package repo_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/internal/secrets"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type myConfig struct {
	Password *secrets.Secret `json:"password"`
}

type myStorage struct {
	blob.Storage

	cfg    *myConfig
	create bool
}

func TestLocalConfigSecret(t *testing.T) {
	oldConfig := `
	{
		"storage": {
			"type": "myconfig",
			"config": {
				"password": "pass"
			}
		}
	}`

	newConfigSecure := `
	{
		"storage": {
			"type": "myconfig",
			"config": {
				"password": {
					"encrypted": "92dab58ce3721da64f14bc874f161183770611a9498e4834a2d31d0944d7424a"
				}
			}
		},
		"encryptedToken": {
			"key": "6808aed4f8cc74506cc9d795ab4322d027ea352c14cbbe4cf22255b335fece8398375eb7bb6ed7e3ecc6e62baf8a7729891f9066531a3016f53f6610-3c2ed3276e6f6066",
			"algorithm": "AES256-GCM-HMAC-SHA256"
		}
	}`

	newConfigInsecure := `
	{
		"storage": {
			"type": "myconfig",
			"config": {
				"password": {
					"encrypted": "3d28c6dd1e59a89e5b19ec80854b50d1becfb6d9fc462c445518a1a58b4efb61"
				}
			}
		},
		"encryptedToken": {
			"key": "51a7e233c398d8ed9a5fbdb525523ab7b2aaa48a907e8724cae153f57e874924f62017ad0f3770505c7e67ed2603b1a2f619e418967a3791f055e5b9-f44306da6f38c0c7",
			"algorithm": "AES256-GCM-HMAC-SHA256"
		}
	}`

	td := testutil.TempDirectory(t)
	cfgFile := filepath.Join(td, "repository.config")

	blob.AddSupportedStorage("myconfig", myConfig{}, func(c context.Context, mc *myConfig, isCreate bool) (blob.Storage, error) {
		return &myStorage{cfg: mc, create: isCreate}, nil
	})

	// test reading a config with plaintext passwords
	require.NoError(t, os.WriteFile(cfgFile, []byte(oldConfig), 0o600))
	lc, err := repo.LoadConfigFromFile(cfgFile)
	require.NoError(t, err)
	require.NoError(t, secrets.EvaluateSecrets(lc.Storage.Config, &lc.SecretToken, "123"))
	cfg := lc.Storage.Config.(*myConfig)
	require.Equal(t, cfg.Password.String(), "pass")
	require.Equal(t, cfg.Password.Type, secrets.Value)
	require.NotEmpty(t, cfg.Password.StoreValue)
	require.NotEqual(t, cfg.Password.StoreValue, "pass")

	// test reading a config with encrypted passwords
	if crypto.DefaultKeyDerivationAlgorithm == "testing-only-insecure" {
		require.NoError(t, os.WriteFile(cfgFile, []byte(newConfigInsecure), 0o600))
	} else {
		require.NoError(t, os.WriteFile(cfgFile, []byte(newConfigSecure), 0o600))
	}

	lc, err = repo.LoadConfigFromFile(cfgFile)
	require.NoError(t, err)
	secrets.EvaluateSecrets(lc.Storage.Config, &lc.SecretToken, "123")
	cfg = lc.Storage.Config.(*myConfig)
	require.Equal(t, cfg.Password.String(), "pass")
	require.Equal(t, cfg.Password.Type, secrets.Config)
}
