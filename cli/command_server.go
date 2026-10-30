package cli

import (
	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
)

type commandServer struct {
	acl      commandServerACL
	user     commandServerUser
	cancel   commandServerCancel
	flush    commandServerFlush
	pause    commandServerPause
	refresh  commandServerRefresh
	resume   commandServerResume
	start    commandServerStart
	status   commandServerStatus
	throttle commandServerThrottle
	upload   commandServerUpload
	shutdown commandServerShutdown
}

type serverFlags struct {
	serverAddress  string
	serverUsername string
	serverPassword string
}

func (c *serverFlags) setup(svc appServices, cmd *kingpin.CmdClause) {
	cmd.Flag("address", "Server address").Default("http://127.0.0.1:51515").StringVar(&c.serverAddress)
	cmd.Flag("server-username", "HTTP server username (basic auth)").Envar(svc.EnvName("KOPIA_SERVER_USERNAME")).Default("kopia").StringVar(&c.serverUsername)
	cmd.Flag("server-password", "HTTP server password (basic auth)").Envar(svc.EnvName("KOPIA_SERVER_PASSWORD")).StringVar(&c.serverPassword)
}

type serverClientFlags struct {
	serverAddress         string
	serverUsername        string
	serverPassword        string
	serverCertFingerprint string
}

func (c *serverClientFlags) setup(svc appServices, cmd *kingpin.CmdClause) {
	c.serverUsername = defaultServerControlUsername

	cmd.Flag("address", "Address of the server to connect to").Envar(svc.EnvName("KOPIA_SERVER_ADDRESS")).Default("http://127.0.0.1:51515").StringVar(&c.serverAddress)
	cmd.Flag("server-control-username", "Server control username").Envar(svc.EnvName("KOPIA_SERVER_USERNAME")).StringVar(&c.serverUsername)
	cmd.Flag("server-control-password", "Server control password").PlaceHolder("PASSWORD").Envar(svc.EnvName("KOPIA_SERVER_PASSWORD")).StringVar(&c.serverPassword)

	// aliases for backwards compat
	cmd.Flag("server-username", "Server control username").Hidden().StringVar(&c.serverUsername)
	cmd.Flag("server-password", "Server control password").Hidden().StringVar(&c.serverPassword)

	cmd.Flag("server-cert-fingerprint", "Server certificate fingerprint").PlaceHolder("SHA256-FINGERPRINT").Envar(svc.EnvName("KOPIA_SERVER_CERT_FINGERPRINT")).StringVar(&c.serverCertFingerprint)
}

func (c *commandServer) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("server", "Commands to control HTTP API server.")

	c.start.setup(svc, cmd)
	c.acl.setup(svc, cmd)
	c.user.setup(svc, cmd)

	c.status.setup(svc, cmd)
	c.refresh.setup(svc, cmd)
	c.flush.setup(svc, cmd)
	c.shutdown.setup(svc, cmd)

	c.upload.setup(svc, cmd)
	c.cancel.setup(svc, cmd)
	c.pause.setup(svc, cmd)
	c.resume.setup(svc, cmd)
	c.throttle.setup(svc, cmd)
}

func (c *serverClientFlags) serverAPIClientOptions() (apiclient.Options, error) {
	if c.serverAddress == "" {
		return apiclient.Options{}, errors.New("missing server address")
	}

	return apiclient.Options{
		BaseURL:                             c.serverAddress,
		Username:                            c.serverUsername,
		Password:                            c.serverPassword,
		TrustedServerCertificateFingerprint: c.serverCertFingerprint,
	}, nil
}
