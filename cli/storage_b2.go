package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/b2"
)

type storageB2Flags struct {
	b2options b2.Options
}

func (c *storageB2Flags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the B2 bucket").Required().StringVar(&c.b2options.BucketName)
	cmd.Flag("key-id", "Key ID (overrides B2_KEY_ID environment variable)").Required().Envar(svc.EnvName("B2_KEY_ID")).StringVar(&c.b2options.KeyID)
	secretVarWithEnv(cmd.Flag("key", "Secret key (overrides B2_KEY environment variable)"), svc.EnvName("B2_KEY"), &c.b2options.Key)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.b2options.Prefix)
	commonThrottlingFlags(cmd, &c.b2options.Limits)
}

func (c *storageB2Flags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	if !c.b2options.Key.IsSet() {
		return nil, errors.New("Must specify secret key")
	}

	//nolint:wrapcheck
	return b2.New(ctx, &c.b2options, isCreate)
}
