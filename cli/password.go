package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/term"

	"github.com/kopia/kopia/internal/passwordpersist"
)

func askForNewRepositoryPassword(out io.Writer) (string, error) {
	for {
		p1, err := askPass(out, "Enter password to create new repository: ")
		if err != nil {
			return "", errors.Wrap(err, "password entry")
		}

		p2, err := askPass(out, "Re-enter password for verification: ")
		if err != nil {
			return "", errors.Wrap(err, "password verification")
		}

		if p1 != p2 {
			fmt.Fprintln(out, "Passwords don't match!") //nolint:errcheck
		} else {
			return p1, nil
		}
	}
}

func askForChangedRepositoryPassword(out io.Writer) (string, error) {
	for {
		p1, err := askPass(out, "Enter new password: ")
		if err != nil {
			return "", errors.Wrap(err, "password entry")
		}

		p2, err := askPass(out, "Re-enter password for verification: ")
		if err != nil {
			return "", errors.Wrap(err, "password verification")
		}

		if p1 != p2 {
			fmt.Println("Passwords don't match!")
		} else {
			return p1, nil
		}
	}
}

func askForExistingRepositoryPassword(out io.Writer) (string, error) {
	p1, err := askPass(out, "Enter password to open repository: ")
	if err != nil {
		return "", err
	}

	fmt.Fprintln(out) //nolint:errcheck

	return p1, nil
}

func (c *App) setPasswordFromToken(pwd string) {
	c.password = pwd
}

func (c *App) getPasswordFromFlags(ctx context.Context, isCreate, allowPersistent bool) (string, error) {
	switch {
	case c.password != "":
		// password provided via --password flag or KOPIA_PASSWORD environment variable
		return strings.TrimSpace(c.password), nil
	case isCreate:
		// this is a new repository, ask for password
		return askForNewRepositoryPassword(c.stdoutWriter)
	case allowPersistent:
		// try fetching the password from persistent storage specific to the configuration file.
		pass, err := c.passwordPersistenceStrategy().GetPassword(ctx, c.repositoryConfigFileName())
		if err == nil {
			return pass, nil
		}

		if !errors.Is(err, passwordpersist.ErrPasswordNotFound) {
			return "", errors.Wrap(err, "error getting persistent password")
		}
	}

	// fall back to asking for existing password
	return askForExistingRepositoryPassword(c.stdoutWriter)
}

// askPass presents a given prompt and asks the user for password.
func askPass(out io.Writer, prompt string) (string, error) {
	for range 5 {
		fmt.Fprint(out, prompt) //nolint:errcheck

		passBytes, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			return "", errors.Wrap(err, "password prompt error")
		}

		fmt.Fprintln(out) //nolint:errcheck

		if len(passBytes) == 0 {
			continue
		}

		return string(passBytes), nil
	}

	return "", errors.New("can't get password")
}
