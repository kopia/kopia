package cli

import (
	"context"
	"io/ioutil"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

type storageSFTPFlags struct {
	options          sftp.Options
	connectFlat      bool
	embedCredentials bool
}

func (c *storageSFTPFlags) setup(_ storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("path", "Path to the repository in the SFTP/SSH server").Required().StringVar(&c.options.Path)
	cmd.Flag("host", "SFTP/SSH server hostname").Required().StringVar(&c.options.Host)
	cmd.Flag("port", "SFTP/SSH server port").Default("22").IntVar(&c.options.Port)
	cmd.Flag("username", "SFTP/SSH server username").Required().StringVar(&c.options.Username)
	cmd.Flag("keyfile", "path to private key file for SFTP/SSH server").StringVar(&c.options.Keyfile)
	cmd.Flag("key-data", "private key data").StringVar(&c.options.KeyData)
	cmd.Flag("known-hosts", "path to known_hosts file").StringVar(&c.options.KnownHostsFile)
	cmd.Flag("known-hosts-data", "known_hosts file entries").StringVar(&c.options.KnownHostsData)
	cmd.Flag("embed-credentials", "Embed key and known_hosts in Kopia configuration").BoolVar(&c.embedCredentials)

	cmd.Flag("external", "Launch external passwordless SSH command").BoolVar(&c.options.ExternalSSH)
	cmd.Flag("ssh-command", "SSH command").Default("ssh").StringVar(&c.options.SSHCommand)
	cmd.Flag("ssh-args", "Arguments to external SSH command").StringVar(&c.options.SSHArguments)

	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
}

func (c *storageSFTPFlags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	sftpo := c.options

	// nolint:nestif
	if !sftpo.ExternalSSH {
		if c.embedCredentials {
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

	if c.connectFlat {
		sftpo.DirectoryShards = []int{}
	}

	// nolint:wrapcheck
	return sftp.New(ctx, &sftpo)
}
