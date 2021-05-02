package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerResume struct {
	sf serverClientFlags
}

func (c *commandServerResume) setup(app appServices, parent commandParent) {
	cmd := parent.Command("resume", "Resume the scheduled snapshots for one or more sources")
	c.sf.setup(cmd)
	cmd.Action(app.serverAction(&c.sf, c.run))
}

func (c *commandServerResume) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return cli.Post(ctx, "sources/resume", &serverapi.Empty{}, &serverapi.Empty{})
}
