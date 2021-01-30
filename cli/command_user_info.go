package cli

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

var (
	userInfoCommand = userCommands.Command("info", "Info about particular user")
	userInfoName    = userInfoCommand.Arg("username", "The username to look up.").Required().String()
)

func runUserInfo(ctx context.Context, rep repo.DirectRepository) error {
	up, err := user.GetUserProfile(ctx, rep, *userInfoName)
	if err != nil {
		return errors.Wrap(err, "error getting user profile")
	}

	j, err := json.MarshalIndent(up, "", "  ")
	if err != nil {
		return errors.Wrap(err, "error marshaling JSON")
	}

	printStdout("%v", string(j))

	return nil
}

func init() {
	userInfoCommand.Action(directRepositoryReadAction(runUserInfo))
}
