package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot/restore"
)

var (
	snapshotRestoreCommand = snapshotCommands.Command("restore", "Restore a snapshot from the snapshot ID to the given target path")
	snapshotRestoreSnapID  = snapshotRestoreCommand.Arg("id", "Snapshot ID to be restored").Required().String()
)

func runSnapRestoreCommand(ctx context.Context, rep repo.Repository) error {
	output, err := restoreOutput()
	if err != nil {
		return errors.Wrap(err, "unable to initialize output")
	}

	st, err := restore.Snapshot(ctx, rep, output, manifest.ID(*snapshotRestoreSnapID))
	if err != nil {
		return err
	}

	printRestoreStats(st)

	return nil
}

func init() {
	addRestoreFlags(snapshotRestoreCommand)
	snapshotRestoreCommand.Action(repositoryAction(runSnapRestoreCommand))
}
