package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/secrets"
	"github.com/kopia/kopia/repo/blob/sftp"
	"github.com/kopia/kopia/repo/blob/sharded"
)

func TestSFTPOptions(t *testing.T) {
	td := t.TempDir()

	myKeyFile := filepath.Join(td, "my-key")
	myKnownHostsFile := filepath.Join(td, "my-known-hosts")

	require.NoError(t, os.WriteFile(myKeyFile, []byte("fake-key-data"), 0o600))
	require.NoError(t, os.WriteFile(myKnownHostsFile, []byte("fake-known-hosts-data"), 0o600))

	cases := []struct {
		input   storageSFTPFlags
		want    *sftp.Options
		wantErr string
	}{
		// 0
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:           "some-host",
					Port:           222,
					Username:       "user",
					KnownHostsFile: "my-known-hosts",
					Keyfile:        "my-key",
				},
			},
			want: &sftp.Options{
				Host:           "some-host",
				Port:           222,
				Username:       "user",
				KnownHostsFile: mustFileAbs(t, "my-known-hosts"),
				Keyfile:        mustFileAbs(t, "my-key"),
			},
		},
		// 1
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:           "some-host",
					Port:           222,
					Username:       "user",
					Keyfile:        "no-such-file",
					KnownHostsFile: myKnownHostsFile,
				},
				embedCredentials: true,
			},
			wantErr: "unable to read key file",
		},
		// 2
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:           "some-host",
					Port:           222,
					Username:       "user",
					Keyfile:        myKeyFile,
					KnownHostsFile: "no-such-file",
				},
				embedCredentials: true,
			},
			wantErr: "unable to read known hosts file",
		},
		// 3
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:           "some-host",
					Port:           222,
					Username:       "user",
					KnownHostsFile: "my-known-hosts",
				},
			},
			wantErr: "must provide either --sftp-password, --keyfile or --key-data",
		},
		// 4
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:     "some-host",
					Port:     222,
					Username: "user",
					Keyfile:  "my-key",
				},
			},
			wantErr: "must provide either --known-hosts or --known-hosts-data",
		},
		// 5
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:           "some-host",
					Port:           222,
					Username:       "user",
					KnownHostsFile: myKnownHostsFile,
					Keyfile:        myKeyFile,
				},
				embedCredentials: true,
			},
			want: &sftp.Options{
				Host:           "some-host",
				Port:           222,
				Username:       "user",
				KeyData:        "fake-key-data",
				KnownHostsData: "fake-known-hosts-data",
			},
		},
		// 6
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:           "some-host",
					Port:           222,
					Username:       "user",
					KnownHostsFile: "my-known-hosts",
					Keyfile:        "my-key",
				},
				connectFlat: true,
			},
			want: &sftp.Options{
				Host:           "some-host",
				Port:           222,
				Username:       "user",
				KnownHostsFile: mustFileAbs(t, "my-known-hosts"),
				Keyfile:        mustFileAbs(t, "my-key"),
				Options: sharded.Options{
					DirectoryShards: []int{},
				},
			},
		},
		// 7
		{
			input: storageSFTPFlags{
				options: sftp.Options{
					Host:           "some-host",
					Port:           222,
					Username:       "user",
					Password:       secrets.NewSecret("my-password"),
					KnownHostsFile: "my-known-hosts",
				},
				connectFlat: true,
			},
			want: &sftp.Options{
				Host:           "some-host",
				Port:           222,
				Username:       "user",
				KnownHostsFile: mustFileAbs(t, "my-known-hosts"),
				Password:       secrets.NewSecret("my-password"),
				Options: sharded.Options{
					DirectoryShards: []int{},
				},
			},
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			got, err := tc.input.getOptions(2)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

func mustFileAbs(t *testing.T, fname string) string {
	t.Helper()

	result, err := filepath.Abs(fname)
	require.NoError(t, err)

	return result
}
