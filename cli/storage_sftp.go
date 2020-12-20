package cli

import (
	"context"
	"io/ioutil"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

func init() {
	var (
		options          sftp.Options
		connectFlat      bool
		embedCredentials bool
	)

	RegisterStorageConnectFlags(
		"sftp",
		"an sftp storage",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("path", "Path to the repository in the SFTP/SSH server").Required().StringVar(&options.Path)
			cmd.Flag("host", "SFTP/SSH server hostname").Required().StringVar(&options.Host)
			cmd.Flag("port", "SFTP/SSH server port").Default("22").IntVar(&options.Port)
			cmd.Flag("username", "SFTP/SSH server username").Required().StringVar(&options.Username)
			cmd.Flag("keyfile", "path to private key file for SFTP/SSH server").StringVar(&options.Keyfile)
			cmd.Flag("key-data", "private key data").StringVar(&options.KeyData)
			cmd.Flag("known-hosts", "path to known_hosts file").StringVar(&options.KnownHostsFile)
			cmd.Flag("known-hosts-data", "known_hosts file entries").StringVar(&options.KnownHostsData)
			cmd.Flag("embed-credentials", "Embed key and known_hosts in Kopia configuration").BoolVar(&embedCredentials)

			cmd.Flag("external", "Launch external passwordless SSH command").BoolVar(&options.ExternalSSH)
			cmd.Flag("ssh-command", "SSH command").Default("ssh").StringVar(&options.SSHCommand)
			cmd.Flag("ssh-args", "Arguments to external SSH command").StringVar(&options.SSHArguments)

			cmd.Flag("flat", "Use flat directory structure").BoolVar(&connectFlat)
		},
		func(ctx context.Context, isNew bool) (blob.Storage, error) {
			sftpo := options

			// nolint:nestif
			if !sftpo.ExternalSSH {
				if embedCredentials {
					if sftpo.KeyData == "" {
						d, err := ioutil.ReadFile(sftpo.Keyfile)
						if err != nil {
							return nil, errors.Wrap(err, "unable to read key file")
						}

						sftpo.KeyData = string(d)
						sftpo.Keyfile = ""
					}

					if sftpo.KnownHostsData == "" && sftpo.KnownHostsFile != "" {
						d, err := ioutil.ReadFile(sftpo.KnownHostsFile)
						if err != nil {
							return nil, errors.Wrap(err, "unable to read known hosts file")
						}

						sftpo.KnownHostsData = string(d)
						sftpo.KnownHostsFile = ""
					}
				}

				if sftpo.KeyData == "" && sftpo.Keyfile == "" {
					return nil, errors.Errorf("must provide either key file or key data")
				}
			}

			if connectFlat {
				sftpo.DirectoryShards = []int{}
			}

			return sftp.New(ctx, &sftpo)
		})
}
