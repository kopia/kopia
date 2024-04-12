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

	connectAPIServerURL             string
	connectAPIServerCertFingerprint string
	connectAPIServerUseGRPCAPI      bool

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
	cmd.Flag("grpc", "Use GRPC API").Default("true").BoolVar(&c.connectAPIServerUseGRPCAPI)
	cmd.Action(svc.noRepositoryAction(c.run))
}

func (c *commandRepositoryConnectServer) run(ctx context.Context) error {
	as := &repo.APIServerInfo{
		BaseURL:                             strings.TrimSuffix(c.connectAPIServerURL, "/"),
		TrustedServerCertificateFingerprint: strings.ToLower(c.connectAPIServerCertFingerprint),
		DisableGRPC:                         !c.connectAPIServerUseGRPCAPI,
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

	log(ctx).Infof("Connected to repository API Server.")
	c.svc.maybeInitializeUpdateCheck(ctx, c.co)

	return nil
}
