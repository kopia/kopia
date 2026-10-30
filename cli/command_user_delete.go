package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

type commandServerUserDelete struct {
	name string
}

func (c *commandServerUserDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Delete user").Alias("remove").Alias("rm")
	cmd.Arg("username", "The username to delete.").Required().StringVar(&c.name)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandServerUserDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	err := user.DeleteUserProfile(ctx, rep, c.name)
	if err != nil {
		return errors.Wrap(err, "error deleting user profile")
	}

	log(ctx).Infof("User %q deleted.", c.name)

	return nil
}
