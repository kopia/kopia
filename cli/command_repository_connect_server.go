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
)

func runConnectAPIServerCommand(ctx context.Context) error {
	password, err := getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	as := &repo.APIServerInfo{
		BaseURL:                             strings.TrimSuffix(*connectAPIServerURL, "/"),
		TrustedServerCertificateFingerprint: strings.ToLower(*connectAPIServerCertFingerprint),
	}

	configFile := repositoryConfigFileName()
	if err := repo.ConnectAPIServer(ctx, configFile, as, password, connectOptions()); err != nil {
		return errors.Wrap(err, "error connecting to API server")
	}

	log(ctx).Infof("Connected to repository API Server.")
	maybeInitializeUpdateCheck(ctx)

	return nil
}

func init() {
	connectAPIServerCommand.Action(noRepositoryAction(runConnectAPIServerCommand))
}
