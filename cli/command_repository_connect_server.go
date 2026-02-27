package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/repo"
)

type commandRepositoryConnectServer struct {
	co *connectOptions

	connectAPIServerURL                              string
	connectAPIServerCertFingerprint                  string
	connectAPIServerLocalCacheKeyDerivationAlgorithm string
	connectAPIServerClientCertificate                string
	connectAPIServerClientPrivateKey                 string

	svc advancedAppServices
	out textOutput
}

func (c *commandRepositoryConnectServer) setup(svc advancedAppServices, parent commandParent, co *connectOptions) {
	c.co = co
	c.svc = svc
	c.out.setup(svc)

	cmd := parent.Command("server", "Connect to a repository API Server.")
	cmd.Flag("url", "Server URL").Required().StringVar(&c.connectAPIServerURL)
	cmd.Flag("server-cert-fingerprint", "Server certificate fingerprint").StringVar(&c.connectAPIServerCertFingerprint)
	cmd.Flag("client-certificate", "Certificate to be sent to the server").StringVar(&c.connectAPIServerClientCertificate)
	cmd.Flag("client-key", "Private key of the client-certificate. Proves the certificate owner is trustworthy").StringVar(&c.connectAPIServerClientPrivateKey)
	//nolint:lll
	cmd.Flag("local-cache-key-derivation-algorithm", "Key derivation algorithm used to derive the local cache encryption key").Hidden().Default(repo.DefaultServerRepoCacheKeyDerivationAlgorithm).EnumVar(&c.connectAPIServerLocalCacheKeyDerivationAlgorithm, repo.SupportedLocalCacheKeyDerivationAlgorithms()...)
	cmd.Action(svc.noRepositoryAction(c.run))
}

func (c *commandRepositoryConnectServer) run(ctx context.Context) error {
	if (c.connectAPIServerClientCertificate != "" && c.connectAPIServerClientPrivateKey == "") ||
		(c.connectAPIServerClientCertificate == "" && c.connectAPIServerClientPrivateKey != "") {
		return errors.New("Both --client-certificate and --client-key are required for client certificate authentication")
	}

	localCacheKeyDerivationAlgorithm := c.connectAPIServerLocalCacheKeyDerivationAlgorithm

	as := &repo.APIServerInfo{
		BaseURL:                             strings.TrimSuffix(c.connectAPIServerURL, "/"),
		TrustedServerCertificateFingerprint: strings.ToLower(c.connectAPIServerCertFingerprint),
		LocalCacheKeyDerivationAlgorithm:    localCacheKeyDerivationAlgorithm,
		ClientCertificateFile:               c.connectAPIServerClientCertificate,
		ClientPrivateKeyFile:                c.connectAPIServerClientPrivateKey,
	}

	configFile := c.svc.repositoryConfigFileName()
	opt := c.co.toRepoConnectOptions()

	u := opt.Username
	if u == "" {
		u = repo.GetDefaultUserName(ctx)
	}

	h := opt.Hostname
	if h == "" {
		h = repo.GetDefaultHostName(ctx)
	}

	log(ctx).Infof("Connecting to server '%v' as '%v@%v'...", as.BaseURL, u, h)

	var pass string

	if c.connectAPIServerClientCertificate == "" {
		var err error

		pass, err = c.svc.getPasswordFromFlags(ctx, false, false)
		if err != nil {
			return errors.Wrap(err, "getting password")
		}
	}

	if err := passwordpersist.OnSuccess(
		ctx, repo.ConnectAPIServer(ctx, configFile, as, pass, opt),
		c.svc.passwordPersistenceStrategy(), configFile, pass); err != nil {
		return errors.Wrap(err, "error connecting to API server")
	}

	log(ctx).Info("Connected to repository API Server.")
	c.svc.maybeInitializeUpdateCheck(ctx, c.co)

	return nil
}
