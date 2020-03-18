package cli

import (
	"context"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/b2"
)

func init() {
	var b2options b2.Options

	RegisterStorageConnectFlags(
		"b2",
		"a b2 bucket",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("bucket", "Name of the B2 bucket").Required().StringVar(&b2options.BucketName)
			cmd.Flag("key-id", "Key ID (overrides B2_KEY_ID environment variable)").Required().Envar("B2_KEY_ID").StringVar(&b2options.KeyID)
			cmd.Flag("key", "Secret key (overrides B2_KEY environment variable)").Required().Envar("B2_KEY").StringVar(&b2options.Key)
			cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&b2options.Prefix)
			cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&b2options.MaxDownloadSpeedBytesPerSecond)
			cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&b2options.MaxUploadSpeedBytesPerSecond)
		},
		func(ctx context.Context, isNew bool) (blob.Storage, error) {
			return b2.New(ctx, &b2options)
		},
	)
}
