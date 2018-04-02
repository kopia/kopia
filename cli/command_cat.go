package cli

import (
	"context"
	"io"
	"os"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	catCommand     = app.Command("cat", "Displays contents of a repository object.")
	catCommandPath = catCommand.Arg("path", "Path").Required().String()
)

func runCatCommand(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewManager(rep)

	oid, err := parseObjectID(ctx, mgr, *catCommandPath)
	if err != nil {
		return err
	}
	r, err := rep.Objects.Open(ctx, oid)
	if err != nil {
		return err
	}
	defer r.Close() //nolint:errcheck
	_, err = io.Copy(os.Stdout, r)
	return err
}

func init() {
	catCommand.Action(repositoryAction(runCatCommand))
}
