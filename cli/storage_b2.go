package cli

import (
	"context"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/b2"
)

type storageB2Flags struct {
	b2options b2.Options
}

func (c *storageB2Flags) Setup(_ StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the B2 bucket").Required().StringVar(&c.b2options.BucketName)
	cmd.Flag("key-id", "Key ID (overrides B2_KEY_ID environment variable)").Required().Envar("B2_KEY_ID").StringVar(&c.b2options.KeyID)
	cmd.Flag("key", "Secret key (overrides B2_KEY environment variable)").Required().Envar("B2_KEY").StringVar(&c.b2options.Key)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.b2options.Prefix)
	commonThrottlingFlags(cmd, &c.b2options.Limits)
}

func (c *storageB2Flags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	// nolint:wrapcheck
	return b2.New(ctx, &c.b2options)
}
