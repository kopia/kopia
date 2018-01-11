package webdavcli

import (
	"context"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/webdav"
	"gopkg.in/alecthomas/kingpin.v2"
)

var options webdav.Options

func connect(ctx context.Context) (storage.Storage, error) {
	wo := options

	if wo.Username != "" {
		pass, err := cli.AskPass("Enter WebDAV password: ")
		if err != nil {
			return nil, err
		}

		wo.Password = pass
	}
	return webdav.New(ctx, &wo)
}

func init() {
	cli.RegisterStorageConnectFlags(
		"webdav",
		"a WebDAV storage",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("url", "URL of WebDAV server").Required().StringVar(&options.URL)
		},
		connect)
}
