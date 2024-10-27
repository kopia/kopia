package cli

import (
	"context"
	"encoding/base64"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/s3"
)

type storageS3Flags struct {
	s3options       s3.Options
	rootCaPemBase64 string
	rootCaPemPath   string
}

func (c *storageS3Flags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("bucket", "Name of the S3 bucket").Required().StringVar(&c.s3options.BucketName)
	cmd.Flag("endpoint", "Endpoint to use").Default("s3.amazonaws.com").StringVar(&c.s3options.Endpoint)
	cmd.Flag("region", "S3 Region").Default("").StringVar(&c.s3options.Region)
	cmd.Flag("access-key", "Access key ID (overrides AWS_ACCESS_KEY_ID environment variable)").Required().Envar(svc.EnvName("AWS_ACCESS_KEY_ID")).StringVar(&c.s3options.AccessKeyID)
	cmd.Flag("secret-access-key", "Secret access key (overrides AWS_SECRET_ACCESS_KEY environment variable)").Required().Envar(svc.EnvName("AWS_SECRET_ACCESS_KEY")).StringVar(&c.s3options.SecretAccessKey)
	cmd.Flag("session-token", "Session token (overrides AWS_SESSION_TOKEN environment variable)").Envar(svc.EnvName("AWS_SESSION_TOKEN")).StringVar(&c.s3options.SessionToken)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket. Put trailing slash (/) if you want to use prefix as directory. e.g my-backup-dir/ would put repository contents inside my-backup-dir directory").StringVar(&c.s3options.Prefix)
	cmd.Flag("disable-tls", "Disable TLS security (HTTPS)").BoolVar(&c.s3options.DoNotUseTLS)
	cmd.Flag("disable-tls-verification", "Disable TLS (HTTPS) certificate verification").BoolVar(&c.s3options.DoNotVerifyTLS)

	commonThrottlingFlags(cmd, &c.s3options.Limits)

	var pointInTimeStr string

	pitPreAction := func(_ *kingpin.ParseContext) error {
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

	cmd.Flag("root-ca-pem-base64", "Certificate authority in-line (base64 enc.)").Envar(svc.EnvName("ROOT_CA_PEM_BASE64")).PreAction(c.preActionLoadPEMBase64).StringVar(&c.rootCaPemBase64)
	cmd.Flag("root-ca-pem-path", "Certificate authority file path").PreAction(c.preActionLoadPEMPath).StringVar(&c.rootCaPemPath)
}

func (c *storageS3Flags) preActionLoadPEMPath(_ *kingpin.ParseContext) error {
	if len(c.s3options.RootCA) > 0 {
		return errors.New("root-ca-pem-base64 and root-ca-pem-path are mutually exclusive")
	}

	data, err := os.ReadFile(c.rootCaPemPath) //#nosec
	if err != nil {
		return errors.Wrapf(err, "error opening root-ca-pem-path %v", c.rootCaPemPath)
	}

	c.s3options.RootCA = data

	return nil
}

func (c *storageS3Flags) preActionLoadPEMBase64(_ *kingpin.ParseContext) error {
	caContent, err := base64.StdEncoding.DecodeString(c.rootCaPemBase64)
	if err != nil {
		return errors.Wrap(err, "unable to decode CA")
	}

	c.s3options.RootCA = caContent

	return nil
}

func (c *storageS3Flags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	if isCreate && c.s3options.PointInTime != nil && !c.s3options.PointInTime.IsZero() {
		return nil, errors.New("Cannot specify a 'point-in-time' option when creating a repository")
	}

	//nolint:wrapcheck
	return s3.New(ctx, &c.s3options, isCreate)
}
