package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryDisconnect struct{}

func (c *commandRepositoryDisconnect) setup(app appServices, parent commandParent) {
	cmd := parent.Command("disconnect", "Disconnect from a repository.")
	cmd.Action(app.noRepositoryAction(c.run))
}

func (c *commandRepositoryDisconnect) run(ctx context.Context) error {
	removeUpdateState()

	return repo.Disconnect(ctx, repositoryConfigFileName())
}
