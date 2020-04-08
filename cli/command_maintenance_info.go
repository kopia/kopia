package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

var (
	maintenanceInfoCommand = maintenanceCommands.Command("info", "Display maintenance information").Alias("status")
)

func runMaintenanceInfoCommand(ctx context.Context, rep *repo.DirectRepository) error {
	p, err := maintenance.GetParams(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get maintenance params")
	}

	s, err := maintenance.GetSchedule(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get maintenance schedule")
	}

	printStderr("Owner: %v\n", p.Owner)
	printStderr("Quick Cycle:\n")
	displayCycleInfo(&p.QuickCycle, s.NextQuickMaintenanceTime, rep)

	printStderr("Full Cycle:\n")
	displayCycleInfo(&p.FullCycle, s.NextFullMaintenanceTime, rep)

	return nil
}

func displayCycleInfo(c *maintenance.CycleParams, t time.Time, rep *repo.DirectRepository) {
	printStderr("  scheduled: %v\n", c.Enabled)

	if c.Enabled {
		printStderr("  interval: %v\n", c.Interval)

		if rep.Time().Before(t) {
			printStderr("  next run: %v (in %v)\n", formatTimestamp(t), time.Until(t).Truncate(time.Second))
		} else {
			printStderr("  next run: now\n")
		}
	}
}

func init() {
	maintenanceInfoCommand.Action(directRepositoryAction(runMaintenanceInfoCommand))
}
