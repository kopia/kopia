package secrets

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zalando/go-keyring"
)

// TestSecretParser will test various inputs for Secret.
func TestSecretParser(t *testing.T) {
	td := t.TempDir()

	password := "password"
	signingKey := NewSigningKey(DefaultAlgorithm, DefaultKeyDerivation)
	signingKey.Create(password)

	signingKey2 := NewSigningKey(DefaultAlgorithm, DefaultKeyDerivation)
	signingKey2.encryptedKey = signingKey.encryptedKey
	signingKey2.salt = signingKey.salt
	signingKey2.IsSet = true

	var signingKeyPtr *EncryptedToken

	cmdFile := filepath.Join(td, "filename")
	require.NoError(t, os.WriteFile(cmdFile, []byte("file-password"), 0o600))

	keyring.MockInit()

	username, err := keyringUsername()

	require.NoError(t, err)
	keyring.Set("from-keyring", username, "keyring-password")

	type testSecret struct {
		Key *Secret
	}

	cases := []struct {
		input     string
		wantValue string
		wantType  keyType
		password  string
		envvar    []string
		failEval  bool
	}{
		{
			// 0: simple value
			input:     "secret",
			wantValue: "secret",
			wantType:  Config,
		},
		{
			// 1: using keyword as value
			input:     "plaintext:envvar:foo",
			wantValue: "envvar:foo",
			wantType:  Config,
		},
		{
			// 2: Env variable
			input:     "envvar:MYENV",
			wantValue: "foo",
			envvar:    []string{"MYENV", "foo"},
			wantType:  Unset,
		},
		{
			// 3: From file
			input:     "file:" + cmdFile,
			wantValue: "file-password",
			wantType:  File,
		},
		{
			// 4: From command
			input:     "command:echo cmd-password",
			wantValue: "cmd-password",
			wantType:  Command,
		},
		{
			// 5: From keyring
			input:     "keyring:from-keyring",
			wantValue: "keyring-password",
			wantType:  Command,
		},
		{
			// 6: Bad password
			input:     "secret",
			password:  "bad_password",
			wantValue: "",
			wantType:  Value,
			failEval:  true,
		},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			if tc.wantType == Command && runtime.GOOS == "windows" {
				t.Skip()
			}
			signingKeyPtr = signingKey
			if tc.envvar != nil {
				_ = os.Setenv(tc.envvar[0], tc.envvar[1])
			}
			st := testSecret{Key: NewSecret(tc.input)}
			if tc.password == "" {
				tc.password = password
			} else {
				signingKey2.derivedKey = nil
				signingKeyPtr = signingKey2
			}
			err := st.Key.Evaluate(signingKeyPtr, tc.password)
			if tc.failEval {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, st.Key.Value)
			if tc.wantType != Unset {
				if st.Key.Type == Config || st.Key.Type == Value {
					require.NotEmpty(t, st.Key.StoreValue)
				}
			} else {
				require.Empty(t, st.Key.StoreValue)
				require.Equal(t, st.Key.Value, tc.wantValue)
			}

			v, err := json.Marshal(&st)
			require.NoError(t, err)

			got := testSecret{}
			err = json.Unmarshal(v, &got)
			require.NoError(t, err)

			got.Key.Evaluate(signingKeyPtr, tc.password)

			if tc.wantType == Config {
				require.Equal(t, st.Key.Value, got.Key.Value)
				require.Equal(t, st.Key.StoreValue, got.Key.StoreValue)
				require.Equal(t, got.Key.Type, Config)
			} else {
				require.Zero(t, got.Key.Value)
				require.Zero(t, got.Key.StoreValue)
				require.Equal(t, got.Key.Type, Unset)
			}
		})
	}
}
