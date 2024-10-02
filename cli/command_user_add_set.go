package cli

import (
	"context"
	"io"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

type commandServerUserAddSet struct {
	userAskPassword     bool
	userSetName         string
	userSetPassword     string
	userSetPasswordHash string

	isNew bool // true == 'add', false == 'update'
	out   textOutput
}

func (c *commandServerUserAddSet) setup(svc appServices, parent commandParent, isNew bool) {
	var cmd *kingpin.CmdClause

	c.isNew = isNew

	if isNew {
		cmd = parent.Command("add", "Add new repository user").Alias("create")
	} else {
		cmd = parent.Command("set", "Set password for a repository user.").Alias("update")
	}

	cmd.Flag("ask-password", "Ask for user password").BoolVar(&c.userAskPassword)
	cmd.Flag("user-password", "Password").StringVar(&c.userSetPassword)
	cmd.Flag("user-password-hash", "Password hash").StringVar(&c.userSetPasswordHash)
	cmd.Arg("username", "Username").Required().StringVar(&c.userSetName)
	cmd.Action(svc.repositoryWriterAction(c.runServerUserAddSet))

	c.out.setup(svc)
}

func (c *commandServerUserAddSet) getExistingOrNewUserProfile(ctx context.Context, rep repo.Repository, username string) (*user.Profile, error) {
	if c.isNew {
		up, err := user.GetNewProfile(ctx, rep, username)

		return up, errors.Wrap(err, "error getting new user profile")
	}

	up, err := user.GetUserProfile(ctx, rep, username)

	return up, errors.Wrap(err, "error getting user profile")
}

func (c *commandServerUserAddSet) runServerUserAddSet(ctx context.Context, rep repo.RepositoryWriter) error {
	username := c.userSetName

	up, err := c.getExistingOrNewUserProfile(ctx, rep, username)
	if err != nil {
		return err
	}

	changed := false

	if p := c.userSetPassword; p != "" {
		changed = true

		if err := up.SetPassword(p); err != nil {
			return errors.Wrap(err, "error setting password")
		}
	}

	if ph := c.userSetPasswordHash; ph != "" {
		if err := up.SetPasswordHash(ph); err != nil {
			return errors.Wrap(err, "error setting password hash")
		}

		changed = true
	}

	if up.PasswordHash == nil || c.userAskPassword {
		pwd, err := askConfirmPass(c.out.stdout(), "Enter new password for user "+username+": ")
		if err != nil {
			return err
		}

		changed = true

		if err := up.SetPassword(pwd); err != nil {
			return errors.Wrap(err, "error setting password")
		}
	}

	if !changed && !c.isNew {
		return errors.New("no change")
	}

	if err := user.SetUserProfile(ctx, rep, up); err != nil {
		return errors.Wrap(err, "error setting user profile")
	}

	log(ctx).Infof(`
Updated user credentials will take effect in 5-10 minutes or when the server is restarted.
To refresh credentials in a running server use 'kopia server refresh' command.
`)

	return nil
}

func askConfirmPass(out io.Writer, initialPrompt string) (string, error) {
	pwd, err := askPass(out, initialPrompt)
	if err != nil {
		return "", errors.Wrap(err, "error asking for password")
	}

	pwd2, err := askPass(out, "Re-enter password for verification: ")
	if err != nil {
		return "", errors.Wrap(err, "error asking for password")
	}

	if pwd != pwd2 {
		return "", errors.Wrap(err, "passwords don't match")
	}

	return pwd, nil
}
