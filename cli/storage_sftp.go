package cli

import (
	"context"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sftp"
)

func init() {
	var (
		options     sftp.Options
		connectFlat bool
	)

	RegisterStorageConnectFlags(
		"sftp",
		"an sftp storage",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("path", "Path to the repository in the SFTP/SSH server").Required().StringVar(&options.Path)
			cmd.Flag("host", "SFTP/SSH server hostname").Required().StringVar(&options.Host)
			cmd.Flag("port", "SFTP/SSH server port").Default("22").IntVar(&options.Port)
			cmd.Flag("username", "SFTP/SSH server username").Required().StringVar(&options.Username)
			cmd.Flag("keyfile", "path to private key file for SFTP/SSH server").Required().StringVar(&options.Keyfile)
			cmd.Flag("flat", "Use flat directory structure").BoolVar(&connectFlat)
		},
		func(ctx context.Context, isNew bool) (blob.Storage, error) {
			sftpo := options

			if connectFlat {
				sftpo.DirectoryShards = []int{}
			}

			return sftp.New(ctx, &sftpo)
		})
}
