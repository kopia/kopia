package cli

import (
	"context"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/webdav"
)

func init() {
	var (
		options     webdav.Options
		connectFlat bool
	)

	RegisterStorageConnectFlags(
		"webdav",
		"a WebDAV storage",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("url", "URL of WebDAV server").Required().StringVar(&options.URL)
			cmd.Flag("flat", "Use flat directory structure").BoolVar(&connectFlat)
			cmd.Flag("webdav-username", "WebDAV username").Envar("KOPIA_WEBDAV_USERNAME").StringVar(&options.Username)
			cmd.Flag("webdav-password", "WebDAV password").Envar("KOPIA_WEBDAV_PASSWORD").StringVar(&options.Password)
		},
		func(ctx context.Context, isNew bool) (blob.Storage, error) {
			wo := options

			if wo.Username != "" && wo.Password == "" {
				pass, err := askPass("Enter WebDAV password: ")
				if err != nil {
					return nil, err
				}

				wo.Password = pass
			}

			if connectFlat {
				wo.DirectoryShards = []int{}
			}

			return webdav.New(ctx, &wo)
		})
}
