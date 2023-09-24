package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryChangePassword struct {
	newPassword string

	svc advancedAppServices
}

func (c *commandRepositoryChangePassword) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("change-password", "Change repository password")
	cmd.Flag("new-password", "New password").Envar(svc.EnvName("KOPIA_NEW_PASSWORD")).StringVar(&c.newPassword)

	c.svc = svc
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryChangePassword) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	var newPass string

	if c.newPassword == "" {
		n, err := askForChangedRepositoryPassword(c.svc.stdout())
		if err != nil {
			return err
		}

		newPass = n
	} else {
		newPass = c.newPassword
	}

	if err := rep.FormatManager().ChangePassword(ctx, newPass, rep); err != nil {
		return errors.Wrap(err, "unable to change password")
	}

	log(ctx).Infof(`NOTE: Repository password has been changed.`)

	if err := c.svc.passwordPersistenceStrategy().PersistPassword(ctx, c.svc.repositoryConfigFileName(), newPass); err != nil {
		return errors.Wrap(err, "unable to persist password")
	}

	return nil
}
