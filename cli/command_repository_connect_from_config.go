package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/kopia/repo"
	"github.com/kopia/repo/storage"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var connectToStorageFromConfigPath string

func connectToStorageFromConfig(ctx context.Context) (storage.Storage, error) {
	var cfg repo.LocalConfig

	f, err := os.Open(connectToStorageFromConfigPath)
	if err != nil {
		return nil, fmt.Errorf("unable to open config: %v", err)
	}
	defer f.Close()

	if err := cfg.Load(f); err != nil {
		return nil, fmt.Errorf("unable to load config: %v", err)
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
