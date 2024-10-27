package cli

import (
	"context"
	"os"
	"path/filepath"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

type storageSFTPFlags struct {
	options          sftp.Options
	connectFlat      bool
	embedCredentials bool
}

func (c *storageSFTPFlags) Setup(_ StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("path", "Path to the repository in the SFTP/SSH server").Required().StringVar(&c.options.Path)
	cmd.Flag("host", "SFTP/SSH server hostname").Required().StringVar(&c.options.Host)
	cmd.Flag("port", "SFTP/SSH server port").Default("22").IntVar(&c.options.Port)
	cmd.Flag("username", "SFTP/SSH server username").Required().StringVar(&c.options.Username)

	// one of those 3 must be provided
	cmd.Flag("sftp-password", "SFTP/SSH server password").StringVar(&c.options.Password)
	cmd.Flag("keyfile", "path to private key file for SFTP/SSH server").StringVar(&c.options.Keyfile)
	cmd.Flag("key-data", "private key data").StringVar(&c.options.KeyData)

	// one of those 2 must be provided
	cmd.Flag("known-hosts", "path to known_hosts file").StringVar(&c.options.KnownHostsFile)
	cmd.Flag("known-hosts-data", "known_hosts file entries").StringVar(&c.options.KnownHostsData)

	cmd.Flag("embed-credentials", "Embed key and known_hosts in Kopia configuration").BoolVar(&c.embedCredentials)

	cmd.Flag("external", "Launch external passwordless SSH command").BoolVar(&c.options.ExternalSSH)
	cmd.Flag("ssh-command", "SSH command").Default("ssh").StringVar(&c.options.SSHCommand)
	cmd.Flag("ssh-args", "Arguments to external SSH command").StringVar(&c.options.SSHArguments)

	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
	cmd.Flag("list-parallelism", "Set list parallelism").Hidden().IntVar(&c.options.ListParallelism)

	commonThrottlingFlags(cmd, &c.options.Limits)
}

func (c *storageSFTPFlags) getOptions(formatVersion int) (*sftp.Options, error) {
	sftpo := c.options

	//nolint:nestif
	if !sftpo.ExternalSSH {
		if c.embedCredentials {
			if sftpo.KeyData == "" {
				d, err := os.ReadFile(sftpo.Keyfile)
				if err != nil {
					return nil, errors.Wrap(err, "unable to read key file")
				}

				sftpo.KeyData = string(d)
				sftpo.Keyfile = ""
			}

			if sftpo.KnownHostsData == "" && sftpo.KnownHostsFile != "" {
				d, err := os.ReadFile(sftpo.KnownHostsFile)
				if err != nil {
					return nil, errors.Wrap(err, "unable to read known hosts file")
				}

				sftpo.KnownHostsData = string(d)
				sftpo.KnownHostsFile = ""
			}
		}

		switch {
		case sftpo.Password != "": // ok

		case sftpo.KeyData != "": // ok

		case sftpo.Keyfile != "":
			a, err := filepath.Abs(sftpo.Keyfile)
			if err != nil {
				return nil, errors.Wrap(err, "error getting absolute path")
			}

			sftpo.Keyfile = a

		default:
			return nil, errors.New("must provide either --sftp-password, --keyfile or --key-data")
		}

		switch {
		case sftpo.KnownHostsData != "": // ok

		case sftpo.KnownHostsFile != "":
			a, err := filepath.Abs(sftpo.KnownHostsFile)
			if err != nil {
				return nil, errors.Wrap(err, "error getting absolute path")
			}

			sftpo.KnownHostsFile = a
		default:
			return nil, errors.New("must provide either --known-hosts or --known-hosts-data")
		}
	}

	sftpo.DirectoryShards = initialDirectoryShards(c.connectFlat, formatVersion)

	return &sftpo, nil
}

func (c *storageSFTPFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	opt, err := c.getOptions(formatVersion)
	if err != nil {
		return nil, err
	}

	//nolint:wrapcheck
	return sftp.New(ctx, opt, isCreate)
}
