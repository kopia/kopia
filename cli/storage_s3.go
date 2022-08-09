package cli

import (
	"context"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/s3"
)

type storageS3Flags struct {
	s3options s3.Options
}

func (c *storageS3Flags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the S3 bucket").Required().StringVar(&c.s3options.BucketName)
	cmd.Flag("endpoint", "Endpoint to use").Default("s3.amazonaws.com").StringVar(&c.s3options.Endpoint)
	cmd.Flag("region", "S3 Region").Default("").StringVar(&c.s3options.Region)
	cmd.Flag("access-key", "Access key ID (overrides AWS_ACCESS_KEY_ID environment variable)").Required().Envar(svc.EnvName("AWS_ACCESS_KEY_ID")).StringVar(&c.s3options.AccessKeyID)
	cmd.Flag("secret-access-key", "Secret access key (overrides AWS_SECRET_ACCESS_KEY environment variable)").Required().Envar(svc.EnvName("AWS_SECRET_ACCESS_KEY")).StringVar(&c.s3options.SecretAccessKey)
	cmd.Flag("session-token", "Session token (overrides AWS_SESSION_TOKEN environment variable)").Envar(svc.EnvName("AWS_SESSION_TOKEN")).StringVar(&c.s3options.SessionToken)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket").StringVar(&c.s3options.Prefix)
	cmd.Flag("disable-tls", "Disable TLS security (HTTPS)").BoolVar(&c.s3options.DoNotUseTLS)
	cmd.Flag("disable-tls-verification", "Disable TLS (HTTPS) certificate verification").BoolVar(&c.s3options.DoNotVerifyTLS)

	commonThrottlingFlags(cmd, &c.s3options.Limits)

	var pointInTimeStr string

	pitPreAction := func(pc *kingpin.ParseContext) error {
		if pointInTimeStr != "" {
			t, err := time.Parse(time.RFC3339, pointInTimeStr)
			if err != nil {
				return errors.Wrap(err, "invalid point-in-time argument")
			}

			c.s3options.PointInTime = &t
		}

		return nil
	}

	cmd.Flag("point-in-time", "Use a point-in-time view of the storage repository when supported").PlaceHolder(time.RFC3339).PreAction(pitPreAction).StringVar(&pointInTimeStr)
}

func (c *storageS3Flags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	if isCreate && c.s3options.PointInTime != nil && !c.s3options.PointInTime.IsZero() {
		return nil, errors.New("Cannot specify a 'point-in-time' option when creating a repository")
	}

	//nolint:wrapcheck
	return s3.New(ctx, &c.s3options)
}
