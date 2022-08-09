package cli

import (
	"context"
	"os"

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

func (c *storageRcloneFlags) Setup(_ StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("remote-path", "Rclone remote:path").Required().StringVar(&c.opt.RemotePath)
	cmd.Flag("flat", "Use flat directory structure").BoolVar(&c.connectFlat)
	cmd.Flag("rclone-exe", "Path to rclone binary").StringVar(&c.opt.RCloneExe)
	cmd.Flag("rclone-args", "Pass additional parameters to rclone").StringsVar(&c.opt.RCloneArgs)
	cmd.Flag("rclone-env", "Pass additional environment (key=value) to rclone").StringsVar(&c.opt.RCloneEnv)
	cmd.Flag("embed-rclone-config", "Embed the provider RClone config").ExistingFileVar(&c.embedRCloneConfigFile)
	cmd.Flag("rclone-debug", "Log rclone output").Hidden().BoolVar(&c.opt.Debug)
	cmd.Flag("rclone-nowait-for-transfers", "Don't wait for transfers when closing storage").Hidden().BoolVar(&c.opt.NoWaitForTransfers)
	cmd.Flag("list-parallelism", "Set list parallelism").Hidden().IntVar(&c.opt.ListParallelism)
	cmd.Flag("atomic-writes", "Assume provider writes are atomic").Default("true").BoolVar(&c.opt.AtomicWrites)

	commonThrottlingFlags(cmd, &c.opt.Limits)
}

func (c *storageRcloneFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	c.opt.DirectoryShards = initialDirectoryShards(c.connectFlat, formatVersion)

	if c.embedRCloneConfigFile != "" {
		cfg, err := os.ReadFile(c.embedRCloneConfigFile)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read rclone config file")
		}

		c.opt.EmbeddedConfig = string(cfg)
	}

	//nolint:wrapcheck
	return rclone.New(ctx, &c.opt, isCreate)
}
