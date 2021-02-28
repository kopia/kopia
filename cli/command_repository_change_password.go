package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryChangePassword struct {
	svc advancedAppServices
}

func (c *commandRepositoryChangePassword) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("change-password", "Change repository password")

	c.svc = svc
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryChangePassword) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	newPass, err := askForChangedRepositoryPassword(c.svc.stdout())
	if err != nil {
		return err
	}

	if err := rep.ChangePassword(ctx, newPass); err != nil {
		return errors.Wrap(err, "unable to change password")
	}

	if err := c.svc.passwordPersistenceStrategy().PersistPassword(ctx, c.svc.repositoryConfigFileName(), newPass); err != nil {
		return errors.Wrap(err, "unable to persist password")
	}

	log(ctx).Infof(`NOTE: Repository password has been changed.`)

	return nil
}
