package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

var userListCommand = userCommands.Command("list", "List users").Alias("ls")

func runUserList(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin()
	defer jl.end()

	profiles, err := user.ListUserProfiles(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error listing user profiles")
	}

	for _, p := range profiles {
		if jsonOutput {
			jl.emit(p)
		} else {
			printStdout("%v\n", p.Username)
		}
	}

	return nil
}

func init() {
	registerJSONOutputFlags(userListCommand)
	userListCommand.Action(repositoryReaderAction(runUserList))
}
