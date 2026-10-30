package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerShutdown struct {
	sf serverClientFlags

	out textOutput
}

func (c *commandServerShutdown) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("shutdown", "Gracefully shutdown the server")
	c.sf.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.serverAction(&c.sf, c.run))
}

func (c *commandServerShutdown) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	//nolint:wrapcheck
	return cli.Post(ctx, "control/shutdown", &serverapi.Empty{}, &serverapi.Empty{})
}
