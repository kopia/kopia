package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
)

type commandServerResume struct {
	commandServerSourceManagerAction
}

func (c *commandServerResume) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("resume", "Resume the scheduled snapshots for one or more sources").Alias("unpause")
	c.commandServerSourceManagerAction.setup(svc, cmd)
	cmd.Action(svc.serverAction(&c.sf, c.run))
}

func (c *commandServerResume) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return c.triggerActionOnMatchingSources(ctx, cli, "control/resume-source")
}
