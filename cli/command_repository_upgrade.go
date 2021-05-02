package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryUpgrade struct{}

func (c *commandRepositoryUpgrade) setup(app appServices, parent commandParent) {
	cmd := parent.Command("upgrade", "Upgrade repository format.")
	cmd.Action(app.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryUpgrade) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	return rep.Upgrade(ctx)
}
