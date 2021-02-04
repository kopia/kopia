package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

var userListCommand = userCommands.Command("list", "List users").Alias("ls")

func runUserList(ctx context.Context, rep repo.Repository) error {
	profiles, err := user.ListUserProfiles(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error listing user profiles")
	}

	for _, p := range profiles {
		printStdout("%v\n", p.Username)
	}

	return nil
}

func init() {
	userListCommand.Action(repositoryReaderAction(runUserList))
}
