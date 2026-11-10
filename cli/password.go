package cli

import (
	"context"
	"fmt"
	"io"
	"math"
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
	case c.passwordFile != "":
		// file containing password provided via --password-file flag or KOPIA_PASSWORD_FILE environment variable
		return passwordFromFile(c.passwordFile)
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
			return "", errors.Wrap(err, "cannot get persistent password")
		}
	}

	// fall back to asking for existing password
	return askForExistingRepositoryPassword(c.stdoutWriter)
}

// passwordFromFile reads the password from the given file
// strips surrounding whitespace including a trailing newline.
func passwordFromFile(fname string) (string, error) {
	data, err := os.ReadFile(fname) //nolint:gosec
	if err != nil {
		return "", errors.Wrap(err, "unable to read password file")
	}

	password := strings.TrimSpace(string(data))
	if password == "" {
		return "", errors.Errorf("password file %q is empty", fname)
	}

	return password, nil
}

// askPass presents a given prompt and asks the user for password.
func askPass(out io.Writer, prompt string) (string, error) {
	fd, err := intFd(os.Stdin)
	if err != nil {
		return "", errors.Wrap(err, "password input error")
	}

	for range 5 {
		fmt.Fprint(out, prompt) //nolint:errcheck

		passBytes, err := term.ReadPassword(fd)
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

var errFdConversionOverflows = errors.New("uintptr file descriptor conversion to int overflows")

func intFd(f *os.File) (int, error) {
	fd := f.Fd()

	if fd <= math.MaxInt {
		return int(fd), nil
	}

	return -1, errFdConversionOverflows
}
