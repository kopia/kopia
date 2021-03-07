package cli

import (
	"context"
	"encoding/base64"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

var (
	userCreateCommand = userCommands.Command("add", "Add new repository user").Alias("create")
	userUpdateCommand = userCommands.Command("set", "Set password for a repository user.").Alias("update")

	userAskPassword            bool
	userSetName                string
	userSetPassword            string
	userSetPasswordHashVersion = 1
	userSetPasswordHash        string
)

func registerAddSetUserCommandArguments(cmd *kingpin.CmdClause) {
	cmd.Flag("ask-password", "Ask for user password").BoolVar(&userAskPassword)
	cmd.Flag("user-password", "Password").StringVar(&userSetPassword)
	cmd.Flag("user-password-hash", "Password hash").StringVar(&userSetPasswordHash)
	cmd.Flag("user-password-hash-version", "Password hash version").Default("1").IntVar(&userSetPasswordHashVersion)
	cmd.Arg("username", "Username").Required().StringVar(&userSetName)
}

func runUserCreate(ctx context.Context, rep repo.RepositoryWriter) error {
	return runServerUserAddSet(ctx, rep, true)
}

func runUserUpdate(ctx context.Context, rep repo.RepositoryWriter) error {
	return runServerUserAddSet(ctx, rep, false)
}

func getExistingOrNewUserProfile(ctx context.Context, rep repo.Repository, username string, isNew bool) (*user.Profile, error) {
	up, err := user.GetUserProfile(ctx, rep, username)

	if isNew {
		switch {
		case err == nil:
			return nil, errors.Errorf("user %q already exists", username)

		case errors.Is(err, user.ErrUserNotFound):
			return &user.Profile{
				Username: username,
			}, nil
		}
	}

	return up, errors.Wrap(err, "error getting user profile")
}

func runServerUserAddSet(ctx context.Context, rep repo.RepositoryWriter, isNew bool) error {
	username := userSetName

	up, err := getExistingOrNewUserProfile(ctx, rep, username, isNew)
	if err != nil {
		return err
	}

	changed := false

	if p := userSetPassword; p != "" {
		changed = true

		if err := up.SetPassword(p); err != nil {
			return errors.Wrap(err, "error setting password")
		}
	}

	if p := userSetPasswordHash; p != "" {
		ph, err := base64.StdEncoding.DecodeString(p)
		if err != nil {
			return errors.Wrap(err, "invalid password hash, must be valid base64 string")
		}

		up.PasswordHashVersion = userSetPasswordHashVersion
		up.PasswordHash = ph
		changed = true
	}

	if up.PasswordHash == nil || userAskPassword {
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

		changed = true

		if err := up.SetPassword(pwd); err != nil {
			return errors.Wrap(err, "error setting password")
		}
	}

	if !changed && !isNew {
		return errors.Errorf("no change")
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
