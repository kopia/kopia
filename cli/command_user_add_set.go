package cli

import (
	"context"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

var (
	userCreateCommand = userCommands.Command("add", "Add new repository user").Alias("create")
	userUpdateCommand = userCommands.Command("set", "Set password for a repository user.").Alias("update")

	userAskPassword     bool
	userSetName         string
	userSetPassword     string
	userSetPasswordHash string
)

func registerAddSetUserCommandArguments(cmd *kingpin.CmdClause) {
	cmd.Flag("ask-password", "Ask for user password").BoolVar(&userAskPassword)
	cmd.Flag("user-password", "Password").StringVar(&userSetPassword)
	cmd.Flag("user-password-hash", "Password hash").StringVar(&userSetPasswordHash)
	cmd.Arg("username", "Username").Required().StringVar(&userSetName)
}

func runUserCreate(ctx context.Context, rep repo.RepositoryWriter) error {
	return runServerUserAddSet(ctx, rep, true)
}

func runUserUpdate(ctx context.Context, rep repo.RepositoryWriter) error {
	return runServerUserAddSet(ctx, rep, false)
}

func runServerUserAddSet(ctx context.Context, rep repo.RepositoryWriter, isNew bool) error {
	username := userSetName

	up, err := user.GetUserProfile(ctx, rep, username)

	if isNew {
		switch {
		case err == nil:
			return errors.Errorf("user %q already exists", username)

		case errors.Is(err, user.ErrUserNotFound):
			up = &user.Profile{
				Username: username,
			}
			err = nil
		}
	}

	if err != nil {
		return errors.Wrap(err, "error getting user profile")
	}

	if p := userSetPassword; p != "" {
		if err := up.SetPassword(p); err != nil {
			return errors.Wrap(err, "error setting password")
		}
	}

	if p := userSetPasswordHash; p != "" {
		up.PasswordHash = p
	}

	if up.PasswordHash == "" || userAskPassword {
		pwd, err := askPass("Enter new password for user " + username + ": ")
		if err != nil {
			return errors.Wrap(err, "error asking for password")
		}

		pwd2, err := askPass("Re-enter new password for verification: ")
		if err != nil {
			return errors.Wrap(err, "error asking for password")
		}

		if pwd != pwd2 {
			return errors.Wrap(err, "passwords don't match")
		}

		if err := up.SetPassword(pwd); err != nil {
			return errors.Wrap(err, "error setting password")
		}
	}

	if err := user.SetUserProfile(ctx, rep, up); err != nil {
		return errors.Wrap(err, "error setting user profile")
	}

	log(ctx).Noticef(`
Updated user credentials will take effect in 5-10 minutes or when the server is restarted.
To refresh credentials in a running server use 'kopia server refresh' command.
`)

	return nil
}

func init() {
	registerAddSetUserCommandArguments(userCreateCommand)
	registerAddSetUserCommandArguments(userUpdateCommand)
	userCreateCommand.Action(repositoryWriterAction(runUserCreate))
	userUpdateCommand.Action(repositoryWriterAction(runUserUpdate))
}
