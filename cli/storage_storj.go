package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/storj"
)

type storageStorjFlags struct {
	options storj.Options
}

func (c *storageStorjFlags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the Storj bucket").Required().StringVar(&c.options.BucketName)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.options.Prefix)
	cmd.Flag("access-grant", "Access grant (overrides STORJ_ACCESS_GRANT environment variable)").Required().Envar(svc.EnvName("STORJ_ACCESS_GRANT")).StringVar(&c.options.AccessGrant)
	commonThrottlingFlags(cmd, &c.options.Limits)
}

func (c *storageStorjFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	//nolint:wrapcheck
	return storj.New(ctx, &c.options, isCreate)
}

func init() {
	mustRegisterStorageProvider(
		"storj",
		"a Storj storage",
		func() StorageFlags { return &storageStorjFlags{} },
	)
}
