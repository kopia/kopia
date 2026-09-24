package cli

import (
	"context"
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/passwordpersist"
	"github.com/kopia/kopia/repo"
)

type commandRepositoryConnectServer struct {
	co *connectOptions

	connectAPIServerURL                              string
	connectAPIServerCertFingerprint                  string
	connectAPIServerCertCAFile                       string
	connectAPIServerLocalCacheKeyDerivationAlgorithm string

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
	cmd.Flag("server-cert-ca-file", "Path to a server CA certificate(s) PEM file; alternative to --server-cert-fingerprint").StringVar(&c.connectAPIServerCertCAFile)
	//nolint:lll
	cmd.Flag("local-cache-key-derivation-algorithm", "Key derivation algorithm used to derive the local cache encryption key").Hidden().Default(repo.DefaultServerRepoCacheKeyDerivationAlgorithm).EnumVar(&c.connectAPIServerLocalCacheKeyDerivationAlgorithm, repo.SupportedLocalCacheKeyDerivationAlgorithms()...)
	cmd.Action(svc.noRepositoryAction(c.run))
}

func (c *commandRepositoryConnectServer) run(ctx context.Context) error {
	localCacheKeyDerivationAlgorithm := c.connectAPIServerLocalCacheKeyDerivationAlgorithm

	if c.connectAPIServerCertFingerprint != "" && c.connectAPIServerCertCAFile != "" {
		return errors.New("server-cert-fingerprint and server-cert-ca-file are mutually exclusive")
	}

	var caPEM []byte

	if c.connectAPIServerCertCAFile != "" {
		data, err := os.ReadFile(c.connectAPIServerCertCAFile) //#nosec
		if err != nil {
			return errors.Wrapf(err, "error opening server-cert-ca-file %v", c.connectAPIServerCertCAFile)
		}

		caPEM = data
	}

	as := &repo.APIServerInfo{
		BaseURL:                             strings.TrimSuffix(c.connectAPIServerURL, "/"),
		TrustedServerCertificateFingerprint: strings.ToLower(c.connectAPIServerCertFingerprint),
		TrustedServerCACertificate:          caPEM,
		LocalCacheKeyDerivationAlgorithm:    localCacheKeyDerivationAlgorithm,
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

	pass, err := c.svc.getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
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
