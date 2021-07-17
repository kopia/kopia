package cli

import (
	"context"
	"io/ioutil"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/rclone"
)

type storageRcloneFlags struct {
	opt                   rclone.Options
	connectFlat           bool
	embedRCloneConfigFile string
}

func (c *storageRcloneFlags) setup(_ storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("remote-path", "Rclone remote:path").Required().StringVar(&c.opt.RemotePath)
	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
	cmd.Flag("rclone-exe", "Path to rclone binary").StringVar(&c.opt.RCloneExe)
	cmd.Flag("rclone-args", "Pass additional parameters to rclone").StringsVar(&c.opt.RCloneArgs)
	cmd.Flag("rclone-env", "Pass additional environment (key=value) to rclone").StringsVar(&c.opt.RCloneEnv)
	cmd.Flag("embed-rclone-config", "Embed the provider RClone config").ExistingFileVar(&c.embedRCloneConfigFile)
	cmd.Flag("rclone-debug", "Log rclone output").Hidden().BoolVar(&c.opt.Debug)
	cmd.Flag("rclone-nowait-for-transfers", "Don't wait for transfers when closing storage").Hidden().BoolVar(&c.opt.NoWaitForTransfers)
}

func (c *storageRcloneFlags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	if c.connectFlat {
		c.opt.DirectoryShards = []int{}
	}

	if c.embedRCloneConfigFile != "" {
		cfg, err := ioutil.ReadFile(c.embedRCloneConfigFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read rclone config file")
		}

		c.opt.EmbeddedConfig = string(cfg)
	}

	// nolint:wrapcheck
	return rclone.New(ctx, &c.opt)
}
