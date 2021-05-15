package cli

import (
	"context"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type storageFromConfigFlags struct {
	connectFromConfigFile  string
	connectFromConfigToken string

	sps storageProviderServices
}

func (c *storageFromConfigFlags) setup(sps storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("file", "Path to the configuration file").StringVar(&c.connectFromConfigFile)
	cmd.Flag("token", "Configuration token").StringVar(&c.connectFromConfigToken)

	c.sps = sps
}

func (c *storageFromConfigFlags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	if isNew {
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

	// nolint:wrapcheck
	return blob.NewStorage(ctx, *cfg.Storage)
}

func (c *storageFromConfigFlags) connectToStorageFromConfigToken(ctx context.Context) (blob.Storage, error) {
	ci, pass, err := repo.DecodeToken(c.connectFromConfigToken)
	if err != nil {
		return nil, errors.Wrap(err, "invalid token")
	}

	if pass != "" {
		c.sps.setPasswordFromToken(pass)
	}

	// nolint:wrapcheck
	return blob.NewStorage(ctx, ci)
}
