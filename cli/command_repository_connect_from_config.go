package cli

import (
	"context"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type storageFromConfigFlags struct {
	connectFromConfigFile  string
	connectFromConfigToken string
	connectFromTokenFile   string

	sps StorageProviderServices
}

func (c *storageFromConfigFlags) Setup(sps StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("file", "Path to the configuration file").StringVar(&c.connectFromConfigFile)
	cmd.Flag("token", "Configuration token").StringVar(&c.connectFromConfigToken)
	cmd.Flag("token-file", "Path to the configuration token file").StringVar(&c.connectFromTokenFile)

	c.sps = sps
}

func (c *storageFromConfigFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	if isCreate {
		return nil, errors.New("not supported")
	}

	if c.connectFromConfigFile != "" {
		return c.connectToStorageFromConfigFile(ctx)
	}

	if c.connectFromConfigToken != "" {
		return c.connectToStorageFromConfigToken(ctx, c.connectFromConfigToken)
	}

	if c.connectFromTokenFile != "" {
		return c.connectToStorageFromStorageConfigFile(ctx)
	}

	if isCreate {
		return nil, errors.New("either --token-file or --token must be provided")
	}

	return nil, errors.New("one of --file, --token-file or --token must be provided")
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

func (c *storageFromConfigFlags) connectToStorageFromConfigToken(ctx context.Context, token string) (blob.Storage, error) {
	ci, pass, err := repo.DecodeToken(token)
	if err != nil {
		return nil, errors.Wrap(err, "invalid token")
	}

	if pass != "" {
		c.sps.setPasswordFromToken(pass)
	}

	//nolint:wrapcheck
	return blob.NewStorage(ctx, ci, false)
}

func (c *storageFromConfigFlags) connectToStorageFromStorageConfigFile(ctx context.Context) (blob.Storage, error) {
	tokenData, err := os.ReadFile(c.connectFromTokenFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open token file")
	}

	return c.connectToStorageFromConfigToken(ctx, string(tokenData))
}
