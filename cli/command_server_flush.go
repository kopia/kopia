package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerFlush struct {
	sf serverClientFlags
}

func (c *commandServerFlush) setup(parent commandParent) {
	cmd := parent.Command("flush", "Flush the state of Kopia server to persistent storage, etc.")
	c.sf.setup(cmd)
	cmd.Action(serverAction(&c.sf, c.run))
}

func (c *commandServerFlush) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return cli.Post(ctx, "flush", &serverapi.Empty{}, &serverapi.Empty{})
}
