package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryDisconnect struct{}

func (c *commandRepositoryDisconnect) setup(parent commandParent) {
	cmd := parent.Command("disconnect", "Disconnect from a repository.")
	cmd.Action(noRepositoryAction(c.run))
}

func (c *commandRepositoryDisconnect) run(ctx context.Context) error {
	removeUpdateState()

	return repo.Disconnect(ctx, repositoryConfigFileName())
}
