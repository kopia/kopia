package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryUpgrade struct{}

func (c *commandRepositoryUpgrade) setup(parent commandParent) {
	cmd := parent.Command("upgrade", "Upgrade repository format.")
	cmd.Action(directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryUpgrade) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	return rep.Upgrade(ctx)
}
