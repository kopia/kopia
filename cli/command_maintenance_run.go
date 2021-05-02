package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

type commandMaintenanceRun struct {
	maintenanceRunFull  bool
	maintenanceRunForce bool
	safety              maintenance.SafetyParameters
}

func (c *commandMaintenanceRun) setup(app appServices, parent commandParent) {
	cmd := parent.Command("run", "Run repository maintenance").Default()
	cmd.Flag("full", "Full maintenance").BoolVar(&c.maintenanceRunFull)
	cmd.Flag("force", "Run maintenance even if not owned (unsafe)").Hidden().BoolVar(&c.maintenanceRunForce)
	safetyFlagVar(cmd, &c.safety)

	cmd.Action(app.directRepositoryWriteAction(c.run))
}

func (c *commandMaintenanceRun) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	mode := maintenance.ModeQuick
	if c.maintenanceRunFull {
		mode = maintenance.ModeFull
	}

	return snapshotmaintenance.Run(ctx, rep, mode, c.maintenanceRunForce, c.safety)
}
