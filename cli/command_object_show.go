package cli

import (
	"context"
	"io"

	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
)

var (
	showCommand = objectCommands.Command("show", "Show contents of a repository object.").Alias("cat")

	showObjectIDs = showCommand.Arg("id", "IDs of objects to show").Required().Strings()
)

func runShowCommand(ctx context.Context, rep *repo.Repository) error {
	for _, oidString := range *showObjectIDs {
		oid, err := parseObjectID(ctx, rep, oidString)
		if err != nil {
			return err
		}

		if err := showObject(ctx, rep, oid); err != nil {
			return err
		}
	}

	return nil
}

func showObject(ctx context.Context, r *repo.Repository, oid object.ID) error {
	var rd io.ReadCloser

	rd, err := r.Objects.Open(ctx, oid)
	if err != nil {
		return err
	}
	defer rd.Close() //nolint:errcheck

	return showContent(rd)
}

func init() {
	setupShowCommand(showCommand)
	showCommand.Action(repositoryAction(runShowCommand))
}
