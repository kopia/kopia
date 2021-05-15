package cli

import (
	"context"
	"os"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/webdav"
)

type storageWebDAVFlags struct {
	options     webdav.Options
	connectFlat bool
}

func (c *storageWebDAVFlags) setup(_ storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("url", "URL of WebDAV server").Required().StringVar(&c.options.URL)
	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
	cmd.Flag("webdav-username", "WebDAV username").Envar("KOPIA_WEBDAV_USERNAME").StringVar(&c.options.Username)
	cmd.Flag("webdav-password", "WebDAV password").Envar("KOPIA_WEBDAV_PASSWORD").StringVar(&c.options.Password)
}

func (c *storageWebDAVFlags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	wo := c.options

	if wo.Username != "" && wo.Password == "" {
		pass, err := askPass(os.Stdout, "Enter WebDAV password: ")
		if err != nil {
			return nil, err
		}

		wo.Password = pass
	}

	if c.connectFlat {
		wo.DirectoryShards = []int{}
	}

	// nolint:wrapcheck
	return webdav.New(ctx, &wo)
}
