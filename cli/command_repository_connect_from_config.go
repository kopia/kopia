package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type storageFromConfigFlags struct {
	connectFromConfigFile  string
	connectFromConfigToken string

	sps StorageProviderServices
}

func (c *storageFromConfigFlags) Setup(sps StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("file", "Path to the configuration file").StringVar(&c.connectFromConfigFile)
	cmd.Flag("token", "Configuration token").StringVar(&c.connectFromConfigToken)

	c.sps = sps
}

func (c *storageFromConfigFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	if isCreate {
		return nil, errors.New("not supported")
	}

	if c.connectFromConfigFile != "" {
		return c.connectToStorageFromConfigFile(ctx)
	}

	if c.connectFromConfigToken != "" {
		return c.connectToStorageFromConfigToken(ctx)
	}

	return nil, errors.New("either --file or --token must be provided")
}

func (c *storageFromConfigFlags) connectToStorageFromConfigFile(ctx context.Context) (blob.Storage, error) {
	cfg, err := repo.LoadConfigFromFile(c.connectFromConfigFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open config")
	}

	if cfg.Storage == nil {
		return nil, errors.Errorf("connection file does not specify blob storage connection parameters, kopia server connections are not supported")
	}

	//nolint:wrapcheck
	return blob.NewStorage(ctx, *cfg.Storage, false)
}

func (c *storageFromConfigFlags) connectToStorageFromConfigToken(ctx context.Context) (blob.Storage, error) {
	ci, pass, err := repo.DecodeToken(c.connectFromConfigToken)
	if err != nil {
		return nil, errors.Wrap(err, "invalid token")
	}

	if pass != "" {
		c.sps.setPasswordFromToken(pass)
	}

	//nolint:wrapcheck
	return blob.NewStorage(ctx, ci, false)
}
