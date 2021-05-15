package cli

import (
	"context"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/s3"
)

type storageS3Flags struct {
	s3options s3.Options
}

func (c *storageS3Flags) setup(_ storageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the S3 bucket").Required().StringVar(&c.s3options.BucketName)
	cmd.Flag("endpoint", "Endpoint to use").Default("s3.amazonaws.com").StringVar(&c.s3options.Endpoint)
	cmd.Flag("region", "S3 Region").Default("").StringVar(&c.s3options.Region)
	cmd.Flag("access-key", "Access key ID (overrides AWS_ACCESS_KEY_ID environment variable)").Required().Envar("AWS_ACCESS_KEY_ID").StringVar(&c.s3options.AccessKeyID)
	cmd.Flag("secret-access-key", "Secret access key (overrides AWS_SECRET_ACCESS_KEY environment variable)").Required().Envar("AWS_SECRET_ACCESS_KEY").StringVar(&c.s3options.SecretAccessKey)
	cmd.Flag("session-token", "Session token (overrides AWS_SESSION_TOKEN environment variable)").Envar("AWS_SESSION_TOKEN").StringVar(&c.s3options.SessionToken)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.s3options.Prefix)
	cmd.Flag("disable-tls", "Disable TLS security (HTTPS)").BoolVar(&c.s3options.DoNotUseTLS)
	cmd.Flag("disable-tls-verification", "Disable TLS (HTTPS) certificate verification").BoolVar(&c.s3options.DoNotVerifyTLS)
	cmd.Flag("max-download-speed", "Limit the download speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&c.s3options.MaxDownloadSpeedBytesPerSecond)
	cmd.Flag("max-upload-speed", "Limit the upload speed.").PlaceHolder("BYTES_PER_SEC").IntVar(&c.s3options.MaxUploadSpeedBytesPerSecond)
}

func (c *storageS3Flags) connect(ctx context.Context, isNew bool) (blob.Storage, error) {
	// nolint:wrapcheck
	return s3.New(ctx, &c.s3options)
}
