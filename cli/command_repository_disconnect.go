package cli

import (
	"context"

	"github.com/pkg/errors"

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

	if err := repo.Disconnect(ctx, c.svc.repositoryConfigFileName()); err != nil {
		return errors.Wrap(err, "unable to disconnect from repository")
	}

	if err := c.svc.passwordPersistenceStrategy().DeletePassword(ctx, c.svc.repositoryConfigFileName()); err != nil {
		return errors.Wrap(err, "unable to remove persisted password")
	}

	return nil
}
