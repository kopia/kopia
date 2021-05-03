package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryDisconnect struct {
	svc advancedAppServices
}

func (c *commandRepositoryDisconnect) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("disconnect", "Disconnect from a repository.")
	cmd.Action(svc.noRepositoryAction(c.run))

	c.svc = svc
}

func (c *commandRepositoryDisconnect) run(ctx context.Context) error {
	c.svc.removeUpdateState()

	return repo.Disconnect(ctx, c.svc.repositoryConfigFileName())
}
