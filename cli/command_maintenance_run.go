package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/gc"
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

	return maintenance.RunExclusive(ctx, rep, mode, func(p maintenance.RunParameters) error {
		if p.Mode == maintenance.ModeFull {
			if _, err := gc.Run(ctx, rep, p.Params.SnapshotGC, true); err != nil {
				return errors.Wrap(err, "error running snapshot GC")
			}
		}

		return maintenance.Run(ctx, p)
	})
}

func init() {
	maintenanceRunCommand.Action(directRepositoryAction(runMaintenanceCommand))
}
