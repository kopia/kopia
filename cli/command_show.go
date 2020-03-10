package cli

import (
	"context"
	"os"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo"
)

var (
	catCommand     = app.Command("show", "Displays contents of a repository object.").Alias("cat")
	catCommandPath = catCommand.Arg("object-path", "Path").Required().String()
)

func runCatCommand(ctx context.Context, rep repo.Repository) error {
	oid, err := parseObjectID(ctx, rep, *catCommandPath)
	if err != nil {
		return err
	}

	r, err := rep.OpenObject(ctx, oid)
	if err != nil {
		return err
	}

	defer r.Close() //nolint:errcheck

	_, err = iocopy.Copy(os.Stdout, r)

	return err
}

func init() {
	catCommand.Action(repositoryAction(runCatCommand))
}
