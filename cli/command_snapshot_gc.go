package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/gc"
)

var (
	snapshotGCCommand       = snapshotCommands.Command("gc", "Remove contents not used by any snapshot")
	snapshotGCMinContentAge = snapshotGCCommand.Flag("min-age", "Minimum content age to allow deletion").Default("24h").Duration()
	snapshotGCDelete        = snapshotGCCommand.Flag("delete", "Delete unreferenced contents").Bool()
)

func runSnapshotGCCommand(ctx context.Context, rep *repo.Repository) error {
	return gc.Run(ctx, rep, *snapshotGCMinContentAge, *snapshotGCDelete)
}

func init() {
	snapshotGCCommand.Action(repositoryAction(runSnapshotGCCommand))
}
