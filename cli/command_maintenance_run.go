package cli

import (
	"context"

	atunits "github.com/alecthomas/units"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

type commandMaintenanceRun struct {
	maintenanceRunFull       bool
	maintenanceRunForce      bool
	safety                   maintenance.SafetyParameters
	indexCompactionMemory    atunits.Base2Bytes
	indexCompactionMemorySet bool
}

func (c *commandMaintenanceRun) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("run", "Run repository maintenance")
	cmd.Flag("full", "Full maintenance").BoolVar(&c.maintenanceRunFull)
	cmd.Flag("force", "Run maintenance even if not owned (unsafe)").Hidden().BoolVar(&c.maintenanceRunForce)
	cmd.Flag("index-compaction-memory", "Override the index compaction working-memory budget for this run (minimum 1MB; requires local caching; default 512MB unless configured)").IsSetByUser(&c.indexCompactionMemorySet).BytesVar(&c.indexCompactionMemory)
	safetyFlagVar(cmd, &c.safety)

	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandMaintenanceRun) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	if c.indexCompactionMemorySet {
		if int64(c.indexCompactionMemory) < indexblob.MinimumEpochCompactionMemoryLimit {
			return errors.New("index-compaction-memory must be at least 1MB")
		}

		ctx = indexblob.WithEpochCompactionOptions(ctx, indexblob.EpochCompactionOptions{
			MemoryLimitBytes: int64(c.indexCompactionMemory),
		})
	}

	mode := maintenance.ModeQuick

	if c.maintenanceRunFull {
		mode = maintenance.ModeFull
	}

	//nolint:wrapcheck
	return snapshotmaintenance.Run(ctx, rep, mode, c.maintenanceRunForce, c.safety)
}
