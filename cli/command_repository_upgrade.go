package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryUpgrade struct{}

func (c *commandRepositoryUpgrade) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("upgrade", "Upgrade repository format.")
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryUpgrade) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	// nolint:wrapcheck
	return rep.Upgrade(ctx)
}
