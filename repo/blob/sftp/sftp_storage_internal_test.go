//go:build !no_extra_providers

package sftp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/jsonencoding"
)

func Test_createSSHConfig_timeout(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{name: "default", timeout: 0, want: defaultConnectTimeout},
		{name: "explicit", timeout: 5 * time.Second, want: 5 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opt := &Options{
				Host:     "localhost",
				Username: "user",
				Password: "password",
				// known_hosts parser skips comment lines; a comment-only file is a valid, empty known_hosts.
				KnownHostsData: "# empty",
				ConnectTimeout: jsonencoding.Duration{Duration: tc.timeout},
			}

			cfg, err := createSSHConfig(testlogging.Context(t), opt)
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Timeout)
		})
	}
}
