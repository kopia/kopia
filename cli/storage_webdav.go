package cli

import (
	"context"

	"github.com/kopia/repo/storage"
	"github.com/kopia/repo/storage/webdav"
	"gopkg.in/alecthomas/kingpin.v2"
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
		},
		func(ctx context.Context) (storage.Storage, error) {
			wo := options

			if wo.Username != "" {
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
