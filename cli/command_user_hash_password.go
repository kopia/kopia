package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/user"
	"github.com/kopia/kopia/repo"
)

type commandServerUserHashPassword struct {
	password string

	out textOutput
}

func (c *commandServerUserHashPassword) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("hash-password", "Hash a user password that can be passed to the 'server user add/set' command").Alias("hash")

	cmd.Flag("user-password", "Password").StringVar(&c.password)

	cmd.Action(svc.repositoryWriterAction(c.runServerUserHashPassword))

	c.out.setup(svc)
}

// The current implementation does not require a connected repository, thus the
// RepositoryWriter parameter is not used. Future implementations will need a
// connected repository. To avoid a future incompatible change where the
// 'hash-password' command stops working without a connected repository,
// a connected repository is required now.
func (c *commandServerUserHashPassword) runServerUserHashPassword(ctx context.Context, _ repo.RepositoryWriter) error {
	if c.password == "" {
		// when password hash is empty, ask for password
		pwd, err := askConfirmPass(c.out.stdout(), "Enter password to hash: ")
		if err != nil {
			return errors.Wrap(err, "error getting password")
		}

		c.password = pwd
	}

	h, err := user.HashPassword(c.password)
	if err != nil {
		return errors.Wrap(err, "hashing password")
	}

	c.out.printStdout("%s\n", h)

	return nil
}
