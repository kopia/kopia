package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerRefresh struct {
	sf serverClientFlags
}

func (c *commandServerRefresh) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("refresh", "Refresh the cache in Kopia server to observe new sources, etc.")
	c.sf.setup(svc, cmd)
	cmd.Action(svc.serverAction(&c.sf, c.run))
}

func (c *commandServerRefresh) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	// nolint:wrapcheck
	return cli.Post(ctx, "control/refresh", &serverapi.Empty{}, &serverapi.Empty{})
}
