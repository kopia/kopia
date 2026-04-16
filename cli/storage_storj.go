package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/storj"
)

type storageStorjFlags struct {
	storjoptions storj.Options
}

func (c *storageStorjFlags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the Storj bucket").Required().StringVar(&c.storjoptions.BucketName)
	cmd.Flag("access-grant", "Storj access grant (overrides STORJ_ACCESS_GRANT environment variable)").Required().Envar(svc.EnvName("STORJ_ACCESS_GRANT")).StringVar(&c.storjoptions.AccessGrant)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.storjoptions.Prefix)
	commonThrottlingFlags(cmd, &c.storjoptions.Limits)
}

func (c *storageStorjFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	//nolint:wrapcheck
	return storj.New(ctx, &c.storjoptions, isCreate)
}
