package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryDisconnect struct {
	app appServices
}

func (c *commandRepositoryDisconnect) setup(app appServices, parent commandParent) {
	cmd := parent.Command("disconnect", "Disconnect from a repository.")
	cmd.Action(app.noRepositoryAction(c.run))

	c.app = app
}

func (c *commandRepositoryDisconnect) run(ctx context.Context) error {
	c.app.removeUpdateState()

	return repo.Disconnect(ctx, c.app.repositoryConfigFileName())
}
