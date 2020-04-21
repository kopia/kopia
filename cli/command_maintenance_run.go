package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

var (
	maintenanceRunCommand = maintenanceCommands.Command("run", "Run repository maintenance").Default()
	maintenanceRunFull    = maintenanceRunCommand.Flag("full", "Full maintenance").Bool()
)

func runMaintenanceCommand(ctx context.Context, rep *repo.DirectRepository) error {
	mode := maintenance.ModeQuick
	if *maintenanceRunFull {
		mode = maintenance.ModeFull
	}

	return snapshotmaintenance.Run(ctx, rep, mode)
}

func init() {
	maintenanceRunCommand.Action(directRepositoryAction(runMaintenanceCommand))
}
