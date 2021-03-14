package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var (
	connectAPIServerCommand = connectCommand.Command("server", "Connect to a repository API Server.")

	connectAPIServerURL             = connectAPIServerCommand.Flag("url", "Server URL").Required().String()
	connectAPIServerCertFingerprint = connectAPIServerCommand.Flag("server-cert-fingerprint", "Server certificate fingerprint").String()
	connectAPIServerUseGRPCAPI      = connectAPIServerCommand.Flag("grpc", "Use GRPC API").Default("true").Bool()
)

func runConnectAPIServerCommand(ctx context.Context) error {
	as := &repo.APIServerInfo{
		BaseURL:                             strings.TrimSuffix(*connectAPIServerURL, "/"),
		TrustedServerCertificateFingerprint: strings.ToLower(*connectAPIServerCertFingerprint),
		DisableGRPC:                         !*connectAPIServerUseGRPCAPI,
	}

	configFile := repositoryConfigFileName()
	opt := connectOptions()

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
	maybeInitializeUpdateCheck(ctx)

	return nil
}

func init() {
	connectAPIServerCommand.Action(noRepositoryAction(runConnectAPIServerCommand))
}
