package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

var (
	userDeleteCommand = userCommands.Command("delete", "Delete user")
	userDeleteName    = userDeleteCommand.Arg("username", "The username to delete.").Required().String()
)

func runUserDelete(ctx context.Context, rep repo.RepositoryWriter) error {
	err := user.DeleteUserProfile(ctx, rep, *userDeleteName)
	if err != nil {
		return errors.Wrap(err, "error deleting user profile")
	}

	log(ctx).Infof("User %q deleted.", *userDeleteName)

	return nil
}

func init() {
	userDeleteCommand.Action(repositoryWriterAction(runUserDelete))
}
