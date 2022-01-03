package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
)

type commandServerCancel struct {
	commandServerSourceManagerAction
}

func (c *commandServerCancel) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("cancel", "Cancels in-progress uploads for one or more sources")
	c.commandServerSourceManagerAction.setup(svc, cmd)
	cmd.Action(svc.serverAction(&c.sf, c.runServerCancelUpload))
}

func (c *commandServerCancel) runServerCancelUpload(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return c.triggerActionOnMatchingSources(ctx, cli, "control/cancel-snapshot")
}
