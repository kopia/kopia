package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
)

type commandServerPause struct {
	commandServerSourceManagerAction
}

func (c *commandServerPause) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("pause", "Pause the scheduled snapshots for one or more sources")
	c.commandServerSourceManagerAction.setup(svc, cmd)
	cmd.Action(svc.serverAction(&c.sf, c.run))
}

func (c *commandServerPause) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return c.triggerActionOnMatchingSources(ctx, cli, "control/pause-source")
}
