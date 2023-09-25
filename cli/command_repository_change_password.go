package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/secrets"
	"github.com/kopia/kopia/repo"
)

type commandRepositoryChangePassword struct {
	newPassword *secrets.Secret

	svc advancedAppServices
}

func (c *commandRepositoryChangePassword) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("change-password", "Change repository password")
	secretVarWithEnv(cmd.Flag("new-password", "New password"), svc.EnvName("KOPIA_NEW_PASSWORD"), &c.newPassword)

	c.svc = svc
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryChangePassword) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	var newPass string

	if !c.newPassword.IsSet() {
		n, err := askForChangedRepositoryPassword(c.svc.stdout())
		if err != nil {
			return err
		}

		newPass = n
	} else {
		_ = c.newPassword.Evaluate(nil, "")
		newPass = c.newPassword.String()
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
