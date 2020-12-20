package cli

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	catCommand     = app.Command("show", "Displays contents of a repository object.").Alias("cat")
	catCommandPath = catCommand.Arg("object-path", "Path").Required().String()
)

func runCatCommand(ctx context.Context, rep repo.Repository) error {
	oid, err := snapshotfs.ParseObjectIDWithPath(ctx, rep, *catCommandPath)
	if err != nil {
		return errors.Wrapf(err, "unable to parse ID: %v", *catCommandPath)
	}

	r, err := rep.OpenObject(ctx, oid)
	if err != nil {
		return errors.Wrapf(err, "error opening object %v", oid)
	}

	defer r.Close() //nolint:errcheck

	_, err = iocopy.Copy(os.Stdout, r)

	return errors.Wrap(err, "unable to copy data")
}

func init() {
	catCommand.Action(repositoryAction(runCatCommand))
}
