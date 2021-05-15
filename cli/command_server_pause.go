package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerPause struct {
	sf serverClientFlags
}

func (c *commandServerPause) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("pause", "Pause the scheduled snapshots for one or more sources")
	c.sf.setup(cmd)
	cmd.Action(svc.serverAction(&c.sf, runServerPause))
}

func runServerPause(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	// nolint:wrapcheck
	return cli.Post(ctx, "sources/pause", &serverapi.Empty{}, &serverapi.Empty{})
}
