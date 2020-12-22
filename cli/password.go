package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/bgentry/speakeasy"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var password = app.Flag("password", "Repository password.").Envar("KOPIA_PASSWORD").Short('p').String()

func askForNewRepositoryPassword() (string, error) {
	for {
		p1, err := askPass("Enter password to create new repository: ")
		if err != nil {
			return "", errors.Wrap(err, "password entry")
		}

		p2, err := askPass("Re-enter password for verification: ")
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

func askForExistingRepositoryPassword() (string, error) {
	p1, err := askPass("Enter password to open repository: ")
	if err != nil {
		return "", err
	}

	fmt.Println()

	return p1, nil
}

var passwordFromToken string

func getPasswordFromFlags(ctx context.Context, isNew, allowPersistent bool) (string, error) {
	if passwordFromToken != "" {
		// password provided via token
		return passwordFromToken, nil
	}

	if !isNew && allowPersistent {
		pass, ok := repo.GetPersistedPassword(ctx, repositoryConfigFileName())
		if ok {
			return pass, nil
		}
	}

	switch {
	case *password != "":
		return strings.TrimSpace(*password), nil
	case isNew:
		return askForNewRepositoryPassword()
	default:
		return askForExistingRepositoryPassword()
	}
}

// askPass presents a given prompt and asks the user for password.
func askPass(prompt string) (string, error) {
	for i := 0; i < 5; i++ {
		p, err := speakeasy.Ask(prompt)
		if err != nil {
			return "", errors.Wrap(err, "password prompt error")
		}

		if p == "" {
			continue
		}

		return p, nil
	}

	return "", errors.New("can't get password")
}
