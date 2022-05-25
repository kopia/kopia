package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotgc"
)

type commandSnapshotGC struct {
	snapshotGCDelete bool
	snapshotGCSafety maintenance.SafetyParameters
}

func (c *commandSnapshotGC) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("gc", "DEPRECATED: This command will be removed. Snapshot GC is now automatically done as part of repository maintence. Snapshot GC marks as deleted all the contents that are not used by any snapshot").Hidden()
	cmd.Flag("delete", "Delete unreferenced contents").BoolVar(&c.snapshotGCDelete)
	safetyFlagVar(cmd, &c.snapshotGCSafety)
	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandSnapshotGC) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	rep.DisableIndexRefresh()

	if err := rep.Refresh(ctx); err != nil {
		return errors.Wrap(err, "error refreshing indexes before snapshot gc")
	}

	st, err := snapshotgc.Run(ctx, rep, c.snapshotGCDelete, c.snapshotGCSafety)

	log(ctx).Infof("GC found %v unused contents (%v bytes)", st.UnusedCount, units.BytesStringBase2(st.UnusedBytes))
	log(ctx).Infof("GC found %v unused contents that are too recent to delete (%v bytes)", st.TooRecentCount, units.BytesStringBase2(st.TooRecentBytes))
	log(ctx).Infof("GC found %v in-use contents (%v bytes)", st.InUseCount, units.BytesStringBase2(st.InUseBytes))
	log(ctx).Infof("GC found %v in-use system-contents (%v bytes)", st.SystemCount, units.BytesStringBase2(st.SystemBytes))

	return errors.Wrap(err, "error running snapshot GC")
}
