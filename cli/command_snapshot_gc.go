package cli

import (
	"context"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/gc"
)

var (
	snapshotGCCommand       = snapshotCommands.Command("gc", "Remove contents not used by any snapshot")
	snapshotGCMinContentAge = snapshotGCCommand.Flag("min-age", "Minimum content age to allow deletion").Default("24h").Duration()
	snapshotGCDelete        = snapshotGCCommand.Flag("delete", "Delete unreferenced contents").Bool()
)

func runSnapshotGCCommand(ctx context.Context, rep *repo.DirectRepository) error {
	st, err := gc.Run(ctx, rep, *snapshotGCMinContentAge, *snapshotGCDelete)

	log(ctx).Infof("GC found %v unused contents (%v bytes)", st.UnusedCount, units.BytesStringBase2(st.UnusedBytes))
	log(ctx).Infof("GC found %v unused contents that are too recent to delete (%v bytes)", st.TooRecentCount, units.BytesStringBase2(st.TooRecentBytes))
	log(ctx).Infof("GC found %v in-use contents (%v bytes)", st.InUseCount, units.BytesStringBase2(st.InUseBytes))
	log(ctx).Infof("GC found %v in-use system-contents (%v bytes)", st.SystemCount, units.BytesStringBase2(st.SystemBytes))

	return err
}

func init() {
	snapshotGCCommand.Action(directRepositoryAction(runSnapshotGCCommand))
}
