package cli

import (
	"context"
	"io/ioutil"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/rclone"
)

func init() {
	var (
		opt                   rclone.Options
		connectFlat           bool
		embedRCloneConfigFile string
	)

	RegisterStorageConnectFlags(
		"rclone",
		"an rclone-based provided",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("remote-path", "Rclone remote:path").Required().StringVar(&opt.RemotePath)
			cmd.Flag("flat", "Use flat directory structure").BoolVar(&connectFlat)
			cmd.Flag("rclone-exe", "Path to rclone binary").StringVar(&opt.RCloneExe)
			cmd.Flag("rclone-args", "Pass additional parameters to rclone").StringsVar(&opt.RCloneArgs)
			cmd.Flag("rclone-env", "Pass additional environment (key=value) to rclone").StringsVar(&opt.RCloneEnv)
			cmd.Flag("embed-rclone-config", "Embed the provider RClone config").ExistingFileVar(&embedRCloneConfigFile)
		},
		func(ctx context.Context, isNew bool) (blob.Storage, error) {
			if connectFlat {
				opt.DirectoryShards = []int{}
			}

			if embedRCloneConfigFile != "" {
				cfg, err := ioutil.ReadFile(embedRCloneConfigFile) //nolint:gosec
				if err != nil {
					return nil, err
				}

				opt.EmbeddedConfig = string(cfg)
			}

			return rclone.New(ctx, &opt)
		},
	)
}
