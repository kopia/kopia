package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/crypto"
	"github.com/kopia/kopia/repo"
)

type commandRepositoryChangePassword struct {
	newPassword   string
	newKDF        string
	newIterations int
	newMemoryMB   int

	svc advancedAppServices
}

func (c *commandRepositoryChangePassword) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("change-password", "Change repository password")
	cmd.Flag("new-password", "New password").Envar(svc.EnvName("KOPIA_NEW_PASSWORD")).StringVar(&c.newPassword)
	cmd.Flag("pbkdf", "Password-based key derivation algorithm (pbkdf2 or scrypt, optional)").Default("").PlaceHolder("ALGO").StringVar(&c.newKDF)
	cmd.Flag("pbkdf-iter", "Number of iterations for PBKDF2 (default: 600000)").Default("0").PlaceHolder("ITERATIONS").IntVar(&c.newIterations)
	cmd.Flag("pbkdf-memory", "Memory cost in MB for scrypt (default: 64MB)").Default("0").PlaceHolder("MB").IntVar(&c.newMemoryMB)

	c.svc = svc
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryChangePassword) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	var newPass string

	if c.newPassword == "" {
		n, err := askForChangedRepositoryPassword(c.svc.stdout())
		if err != nil {
			return err
		}

		newPass = n
	} else {
		newPass = c.newPassword
	}

	// Validate KDF flags are consistent
	if c.newKDF == crypto.PBKDF2 && c.newMemoryMB > 0 {
		return errors.New("--pbkdf-memory cannot be used with --pbkdf=pbkdf2")
	}
	if c.newKDF == crypto.Scrypt && c.newIterations > 0 {
		return errors.New("--pbkdf-iter cannot be used with --pbkdf=scrypt")
	}

	if err := rep.FormatManager().ChangePassword(ctx, newPass, c.newKDF, c.newIterations, c.newMemoryMB); err != nil {
		return errors.Wrap(err, "unable to change password")
	}

	log(ctx).Infof(`NOTE: Repository password has been changed.`)

	if err := c.svc.passwordPersistenceStrategy().PersistPassword(ctx, c.svc.repositoryConfigFileName(), newPass); err != nil {
		return errors.Wrap(err, "unable to persist password")
	}

	return nil
}