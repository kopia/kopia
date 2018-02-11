package cli

import (
	"context"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/s3"
	"gopkg.in/alecthomas/kingpin.v2"
)

var options s3.Options

func connect(ctx context.Context) (storage.Storage, error) {
	return s3.New(ctx, &options)
}

func init() {
	cli.RegisterStorageConnectFlags(
		"s3",
		"an S3 bucket",
		func(cmd *kingpin.CmdClause) {
			cmd.Flag("bucket", "Name of the S3 bucket").Required().StringVar(&options.BucketName)
			cmd.Flag("endpoint", "Endpoint to use").Default("s3.amazonaws.com").StringVar(&options.Endpoint)
			cmd.Flag("access-key", "Access key ID (overrides AWS_ACCESS_KEY_ID environment variable)").Required().Envar("AWS_ACCESS_KEY_ID").StringVar(&options.AccessKeyID)
			cmd.Flag("secret-access-key", "Secret access key (overrides AWS_SECRET_ACCESS_KEY environment variable)").Required().Envar("AWS_SECRET_ACCESS_KEY").StringVar(&options.SecretAccessKey)
			cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&options.Prefix)
			cmd.Flag("disable-tls", "Disable TLS security (HTTPS)").BoolVar(&options.DoNotUseTLS)
			cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&options.MaxDownloadSpeedBytesPerSecond)
			cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&options.MaxUploadSpeedBytesPerSecond)

		},
		connect)
}
