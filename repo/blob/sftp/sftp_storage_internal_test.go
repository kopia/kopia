//go:build !no_extra_providers

package sftp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

const testKnownHostsData = "192.0.2.1 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINWYMIk68blmVjqb8hGGcuthDRTIEJ5V0medl5PM8rW9"

func TestCreateSSHConfigConnectTimeout(t *testing.T) {
	newConfig := func(t *testing.T, connectTimeout time.Duration) *ssh.ClientConfig {
		t.Helper()

		cfg, err := createSSHConfig(context.Background(), &Options{
			Host:           "192.0.2.1",
			Username:       "foo",
			Password:       "password",
			KnownHostsData: testKnownHostsData,
			ConnectTimeout: connectTimeout,
		})
		require.NoError(t, err)

		return cfg
	}

	t.Run("Default", func(t *testing.T) {
		require.Equal(t, defaultSSHConnectTimeout, newConfig(t, 0).Timeout)
	})

	t.Run("Explicit", func(t *testing.T) {
		require.Equal(t, 7*time.Second, newConfig(t, 7*time.Second).Timeout)
	})
}
