package cli

import (
	"context"
	"encoding/base64"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/swift"
)

type storageSwiftFlags struct {
	options         swift.Options
	rootCaPemBase64 string
	rootCaPemPath   string
}

func (c *storageSwiftFlags) Setup(svc StorageProviderServices, cmd *kingpin.CmdClause) {
	cmd.Flag("container", "Name of the Swift container").Required().StringVar(&c.options.ContainerName)
	cmd.Flag("auth-url", "OpenStack Keystone auth URL (overrides OS_AUTH_URL environment variable)").Required().Envar(svc.EnvName("OS_AUTH_URL")).StringVar(&c.options.AuthURL)
	cmd.Flag("username", "OpenStack username (overrides OS_USERNAME environment variable)").Envar(svc.EnvName("OS_USERNAME")).StringVar(&c.options.Username)
	cmd.Flag("user-id", "OpenStack user ID (overrides OS_USER_ID environment variable)").Envar(svc.EnvName("OS_USER_ID")).StringVar(&c.options.UserID)
	cmd.Flag("os-password", "OpenStack password (overrides OS_PASSWORD environment variable)").Envar(svc.EnvName("OS_PASSWORD")).StringVar(&c.options.Password)
	cmd.Flag("domain-name", "OpenStack user domain name (overrides OS_USER_DOMAIN_NAME environment variable)").Envar(svc.EnvName("OS_USER_DOMAIN_NAME")).StringVar(&c.options.DomainName)
	cmd.Flag("domain-id", "OpenStack user domain ID (overrides OS_USER_DOMAIN_ID environment variable)").Envar(svc.EnvName("OS_USER_DOMAIN_ID")).StringVar(&c.options.DomainID)
	cmd.Flag("tenant-name", "OpenStack tenant/project name (overrides OS_PROJECT_NAME environment variable)").Envar(svc.EnvName("OS_PROJECT_NAME")).StringVar(&c.options.TenantName)
	cmd.Flag("tenant-id", "OpenStack tenant/project ID (overrides OS_PROJECT_ID environment variable)").Envar(svc.EnvName("OS_PROJECT_ID")).StringVar(&c.options.TenantID)
	cmd.Flag("token", "OpenStack auth token (overrides OS_TOKEN environment variable)").Envar(svc.EnvName("OS_TOKEN")).StringVar(&c.options.Token)
	cmd.Flag("application-credential-id", "OpenStack application credential ID (overrides OS_APPLICATION_CREDENTIAL_ID environment variable)").Envar(svc.EnvName("OS_APPLICATION_CREDENTIAL_ID")).StringVar(&c.options.ApplicationCredentialID)
	cmd.Flag("application-credential-name", "OpenStack application credential name (overrides OS_APPLICATION_CREDENTIAL_NAME environment variable)").Envar(svc.EnvName("OS_APPLICATION_CREDENTIAL_NAME")).StringVar(&c.options.ApplicationCredentialName)
	cmd.Flag("application-credential-secret", "OpenStack application credential secret (overrides OS_APPLICATION_CREDENTIAL_SECRET environment variable)").Envar(svc.EnvName("OS_APPLICATION_CREDENTIAL_SECRET")).StringVar(&c.options.ApplicationCredentialSecret)
	cmd.Flag("region", "OpenStack region (overrides OS_REGION_NAME environment variable)").Envar(svc.EnvName("OS_REGION_NAME")).StringVar(&c.options.Region)
	cmd.Flag("availability", "OpenStack endpoint availability/interface: public, internal, or admin (overrides OS_INTERFACE environment variable)").Envar(svc.EnvName("OS_INTERFACE")).StringVar(&c.options.Availability)
	cmd.Flag("prefix", "Prefix to use for objects in the container").StringVar(&c.options.Prefix)
	cmd.Flag("read-only", "Connect to storage in read-only mode").BoolVar(&c.options.ReadOnly)
	cmd.Flag("disable-tls-verification", "Disable TLS (HTTPS) certificate verification").BoolVar(&c.options.DoNotVerifyTLS)

	commonThrottlingFlags(cmd, &c.options.Limits)

	cmd.Flag("root-ca-pem-base64", "Certificate authority in-line (base64 enc.)").Envar(svc.EnvName("ROOT_CA_PEM_BASE64")).PreAction(c.preActionLoadPEMBase64).StringVar(&c.rootCaPemBase64)
	cmd.Flag("root-ca-pem-path", "Certificate authority file path").PreAction(c.preActionLoadPEMPath).StringVar(&c.rootCaPemPath)
}

func (c *storageSwiftFlags) preActionLoadPEMPath(_ *kingpin.ParseContext) error {
	if len(c.options.RootCA) > 0 {
		return errors.New("root-ca-pem-base64 and root-ca-pem-path are mutually exclusive")
	}

	data, err := os.ReadFile(c.rootCaPemPath) //#nosec
	if err != nil {
		return errors.Wrapf(err, "error opening root-ca-pem-path %v", c.rootCaPemPath)
	}

	c.options.RootCA = data

	return nil
}

func (c *storageSwiftFlags) preActionLoadPEMBase64(_ *kingpin.ParseContext) error {
	caContent, err := base64.StdEncoding.DecodeString(c.rootCaPemBase64)
	if err != nil {
		return errors.Wrap(err, "unable to decode CA")
	}

	c.options.RootCA = caContent

	return nil
}

func (c *storageSwiftFlags) Connect(ctx context.Context, isCreate bool, formatVersion int) (blob.Storage, error) {
	_ = formatVersion

	//nolint:wrapcheck
	return swift.New(ctx, &c.options, isCreate)
}

func init() {
	mustRegisterStorageProvider(
		"swift",
		"an OpenStack Swift container",
		func() StorageFlags { return &storageSwiftFlags{} },
	)
}
