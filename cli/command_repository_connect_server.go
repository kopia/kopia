package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryConnectServer struct {
	co *connectOptions

	connectAPIServerURL             string
	connectAPIServerCertFingerprint string
	connectAPIServerUseGRPCAPI      bool

	app appServices
}

func (c *commandRepositoryConnectServer) setup(app appServices, parent commandParent, co *connectOptions) {
	c.co = co
	c.app = app

	cmd := parent.Command("server", "Connect to a repository API Server.")
	cmd.Flag("url", "Server URL").Required().StringVar(&c.connectAPIServerURL)
	cmd.Flag("server-cert-fingerprint", "Server certificate fingerprint").StringVar(&c.connectAPIServerCertFingerprint)
	cmd.Flag("grpc", "Use GRPC API").Default("true").BoolVar(&c.connectAPIServerUseGRPCAPI)
	cmd.Action(app.noRepositoryAction(c.run))
}

func (c *commandRepositoryConnectServer) run(ctx context.Context) error {
	as := &repo.APIServerInfo{
		BaseURL:                             strings.TrimSuffix(c.connectAPIServerURL, "/"),
		TrustedServerCertificateFingerprint: strings.ToLower(c.connectAPIServerCertFingerprint),
		DisableGRPC:                         !c.connectAPIServerUseGRPCAPI,
	}

	configFile := repositoryConfigFileName()
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

	password, err := getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	if err := repo.ConnectAPIServer(ctx, configFile, as, password, opt); err != nil {
		return errors.Wrap(err, "error connecting to API server")
	}

	log(ctx).Infof("Connected to repository API Server.")
	c.app.maybeInitializeUpdateCheck(ctx, c.co)

	return nil
}
