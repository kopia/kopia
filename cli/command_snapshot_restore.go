package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	snapshotRestoreCommand    = snapshotCommands.Command("restore", "Restore a snapshot from the snapshot ID to the given target path")
	snapshotRestoreSnapID     = snapshotRestoreCommand.Arg("id", "Snapshot ID to be restored").Required().String()
	snapshotRestoreTargetPath = snapshotRestoreCommand.Arg("target-path", "Path of the directory for the contents to be restored").Required().String()
)

func runSnapRestoreCommand(ctx context.Context, rep repo.Repository) error {
	return snapshotfs.Restore(ctx, rep, *snapshotRestoreTargetPath, manifest.ID(*snapshotRestoreSnapID), restoreOptions())
}

func init() {
	addRestoreFlags(snapshotRestoreCommand)
	snapshotRestoreCommand.Action(repositoryAction(runSnapRestoreCommand))
}
