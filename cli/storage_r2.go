package cli

import (
	"context"
	"encoding/base64"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/r2"
)

type storageR2Flags struct {
	r2options       r2.Options
	rootCaPemBase64 string
	rootCaPemPath   string
}

func (c *storageR2Flags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("account-id", "Cloudflare account ID used to derive the R2 endpoint").Required().Envar(svc.EnvName("R2_ACCOUNT_ID")).StringVar(&c.r2options.AccountID)
	cmd.Flag("bucket", "Name of the Cloudflare R2 bucket").Required().StringVar(&c.r2options.BucketName)
	cmd.Flag("endpoint", "Endpoint to use instead of deriving one from the account ID").StringVar(&c.r2options.Endpoint)
	cmd.Flag("jurisdiction", "Cloudflare R2 jurisdiction for derived endpoints").Default("default").EnumVar(&c.r2options.Jurisdiction, "default", "eu", "fedramp")
	cmd.Flag("access-key", "Access key ID (overrides R2_ACCESS_KEY_ID environment variable)").Required().Envar(svc.EnvName("R2_ACCESS_KEY_ID")).StringVar(&c.r2options.AccessKeyID)
	cmd.Flag("secret-access-key", "Secret access key (overrides R2_SECRET_ACCESS_KEY environment variable)").Required().Envar(svc.EnvName("R2_SECRET_ACCESS_KEY")).StringVar(&c.r2options.SecretAccessKey)
	cmd.Flag("session-token", "Session token (overrides R2_SESSION_TOKEN environment variable)").Envar(svc.EnvName("R2_SESSION_TOKEN")).StringVar(&c.r2options.SessionToken)
	cmd.Flag("prefix", "Prefix to use for objects in the bucket. Put trailing slash (/) if you want to use prefix as directory. e.g my-backup-dir/ would put repository contents inside my-backup-dir directory").StringVar(&c.r2options.Prefix)
	cmd.Flag("disable-tls", "Disable TLS security (HTTPS)").BoolVar(&c.r2options.DoNotUseTLS)
	cmd.Flag("disable-tls-verification", "Disable TLS (HTTPS) certificate verification").BoolVar(&c.r2options.DoNotVerifyTLS)

	commonThrottlingFlags(cmd, &c.r2options.Limits)

	cmd.Flag("root-ca-pem-base64", "Certificate authority in-line (base64 enc.)").Envar(svc.EnvName("ROOT_CA_PEM_BASE64")).PreAction(c.preActionLoadPEMBase64).StringVar(&c.rootCaPemBase64)
	cmd.Flag("root-ca-pem-path", "Certificate authority file path").PreAction(c.preActionLoadPEMPath).StringVar(&c.rootCaPemPath)
}

func (c *storageR2Flags) preActionLoadPEMPath(_ *kingpin.ParseContext) error {
	if len(c.r2options.RootCA) > 0 {
		return errors.New("root-ca-pem-base64 and root-ca-pem-path are mutually exclusive")
	}

	data, err := os.ReadFile(c.rootCaPemPath) //#nosec
	if err != nil {
		return errors.Wrapf(err, "error opening root-ca-pem-path %v", c.rootCaPemPath)
	}

	c.r2options.RootCA = data

	return nil
}

func (c *storageR2Flags) preActionLoadPEMBase64(_ *kingpin.ParseContext) error {
	caContent, err := base64.StdEncoding.DecodeString(c.rootCaPemBase64)
	if err != nil {
		return errors.Wrap(err, "unable to decode CA")
	}

	c.r2options.RootCA = caContent

	return nil
}

func (c *storageR2Flags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	//nolint:wrapcheck
	return r2.New(ctx, &c.r2options, isCreate)
}

func init() {
	mustRegisterStorageProvider(
		"r2",
		"a Cloudflare R2 bucket",
		func() StorageFlags { return &storageR2Flags{} },
	)
}
