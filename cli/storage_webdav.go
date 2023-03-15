package cli

import (
	"context"
	"os"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/webdav"
)

type storageWebDAVFlags struct {
	options     webdav.Options
	connectFlat bool
}

func (c *storageWebDAVFlags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("url", "URL of WebDAV server").Required().StringVar(&c.options.URL)
	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
	cmd.Flag("webdav-username", "WebDAV username").Envar(svc.EnvName("KOPIA_WEBDAV_USERNAME")).StringVar(&c.options.Username)
	cmd.Flag("webdav-password", "WebDAV password").Envar(svc.EnvName("KOPIA_WEBDAV_PASSWORD")).StringVar(&c.options.Password)
	cmd.Flag("list-parallelism", "Set list parallelism").Hidden().IntVar(&c.options.ListParallelism)
	cmd.Flag("atomic-writes", "Assume WebDAV provider implements atomic writes").BoolVar(&c.options.AtomicWrites)

	commonThrottlingFlags(cmd, &c.options.Limits)
}

func (c *storageWebDAVFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	wo := c.options

	if wo.Username != "" && wo.Password == "" {
		pass, err := askPass(os.Stdout, "Enter WebDAV password: ")
		if err != nil {
			return nil, err
		}

		wo.Password = pass
	}

	wo.DirectoryShards = initialDirectoryShards(c.connectFlat, formatVersion)

	//nolint:wrapcheck
	return webdav.New(ctx, &wo, isCreate)
}
