package cli

import (
	"context"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/gcs"
	"gopkg.in/alecthomas/kingpin.v2"
)

var options gcs.Options

func connect(ctx context.Context) (storage.Storage, error) {
	return gcs.New(ctx, &options)
}

func init() {
	cli.RegisterStorageConnectFlags(
		"google",
		"a Google Cloud Storage bucket",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("bucket", "Name of the Google Cloud Storage bucket").Required().StringVar(&options.BucketName)
			cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&options.Prefix)
			cmd.Flag("read-only", "Use read-only GCS scope to prevent write access").BoolVar(&options.ReadOnly)
			cmd.Flag("credentials-file", "Use the provided JSON file with credentials").ExistingFileVar(&options.ServiceAccountCredentials)
			cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&options.MaxDownloadSpeedBytesPerSecond)
			cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&options.MaxUploadSpeedBytesPerSecond)

		},
		connect)
}
