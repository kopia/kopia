package cli

import (
	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
)

type commandServer struct {
	acl     commandServerACL
	user    commandServerUser
	cancel  commandServerCancel
	flush   commandServerFlush
	pause   commandServerPause
	refresh commandServerRefresh
	resume  commandServerResume
	start   commandServerStart
	status  commandServerStatus
	upload  commandServerUpload
}

type serverFlags struct {
	serverAddress  string
	serverUsername string
	serverPassword string
}

func (c *serverFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("address", "Server address").Default("http://127.0.0.1:51515").StringVar(&c.serverAddress)
	cmd.Flag("server-username", "HTTP server username (basic auth)").Envar("KOPIA_SERVER_USERNAME").Default("kopia").StringVar(&c.serverUsername)
	cmd.Flag("server-password", "HTTP server password (basic auth)").Envar("KOPIA_SERVER_PASSWORD").StringVar(&c.serverPassword)
}

type serverClientFlags struct {
	serverFlags
	serverCertFingerprint string
}

func (c *serverClientFlags) setup(cmd *kingpin.CmdClause) {
	c.serverFlags.setup(cmd)
	cmd.Flag("server-cert-fingerprint", "Server certificate fingerprint").StringVar(&c.serverCertFingerprint)
}

func (c *commandServer) setup(app appServices, parent commandParent) {
	cmd := parent.Command("server", "Commands to control HTTP API server.")

	c.cancel.setup(app, cmd)
	c.flush.setup(app, cmd)
	c.pause.setup(app, cmd)
	c.refresh.setup(app, cmd)
	c.resume.setup(app, cmd)
	c.start.setup(app, cmd)
	c.status.setup(app, cmd)
	c.upload.setup(app, cmd)
	c.acl.setup(app, cmd)
	c.user.setup(app, cmd)
}

func (c *serverClientFlags) serverAPIClientOptions() (apiclient.Options, error) {
	if c.serverAddress == "" {
		return apiclient.Options{}, errors.Errorf("missing server address")
	}

	return apiclient.Options{
		BaseURL:                             c.serverAddress,
		Username:                            c.serverUsername,
		Password:                            c.serverPassword,
		TrustedServerCertificateFingerprint: c.serverCertFingerprint,
	}, nil
}
