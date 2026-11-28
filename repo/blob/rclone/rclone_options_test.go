//go:build !no_extra_providers

package rclone_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/scrubber"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/rclone"
)

func TestEmbeddedConfigIsScrubbedFromConnectionInfo(t *testing.T) {
	secret := "[remote]\nuser = example\npass = secret-value\n"
	ci := blob.ConnectionInfo{
		Type: "rclone",
		Config: &rclone.Options{
			RemotePath:     "remote:backup",
			EmbeddedConfig: secret,
		},
	}

	scrubbed := scrubber.ScrubSensitiveData(reflect.ValueOf(ci)).Interface().(blob.ConnectionInfo)
	encoded, err := json.Marshal(scrubbed)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), secret)
	require.Contains(t, string(encoded), strings.Repeat("*", len(secret)))
	require.Contains(t, string(encoded), "remote:backup")

	// Scrubbing the status output must not alter the stored configuration.
	require.Equal(t, secret, ci.Config.(*rclone.Options).EmbeddedConfig)
}
