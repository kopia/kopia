package cli

import (
	"context"
	"os"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/storage"
	"github.com/pkg/errors"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var connectToStorageFromConfigPath string

func connectToStorageFromConfig(ctx context.Context) (storage.Storage, error) {
	var cfg repo.LocalConfig

	f, err := os.Open(connectToStorageFromConfigPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open config")
	}
	defer f.Close() //nolint:errcheck

	if err := cfg.Load(f); err != nil {
		return nil, errors.Wrap(err, "unable to load config")
	}

	return storage.NewStorage(ctx, cfg.Storage)
}

func init() {
	RegisterStorageConnectFlags(
		"from-config",
		"the provided configuration file",
		func(cmd *kingpin.CmdClause) {
			cmd.Arg("file", "Path to the configuration file").Required().StringVar(&connectToStorageFromConfigPath)
		},
		connectToStorageFromConfig)
}
