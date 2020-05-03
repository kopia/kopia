package cli

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

var (
	maintenanceInfoCommand = maintenanceCommands.Command("info", "Display maintenance information").Alias("status")
	maintenanceInfoJSON    = maintenanceInfoCommand.Flag("json", "Show raw JSON data").Short('j').Bool()
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

	if *maintenanceInfoJSON {
		e := json.NewEncoder(os.Stdout)
		e.SetIndent("", "  ")
		e.Encode(s) //nolint:errcheck

		return nil
	}

	printStdout("Owner: %v\n", p.Owner)
	printStdout("Quick Cycle:\n")
	displayCycleInfo(&p.QuickCycle, s.NextQuickMaintenanceTime, rep)

	printStdout("Full Cycle:\n")
	displayCycleInfo(&p.FullCycle, s.NextFullMaintenanceTime, rep)

	printStdout("Recent Maintenance Runs:\n")

	for run, timings := range s.Runs {
		printStdout("  %v:\n", run)

		for _, t := range timings {
			errInfo := ""
			if t.Success {
				errInfo = "SUCCESS"
			} else {
				errInfo = "ERROR: " + t.Error
			}

			printStdout(
				"    %v (%v) %v\n",
				formatTimestamp(t.Start),
				t.End.Sub(t.Start).Truncate(time.Second),
				errInfo)
		}
	}

	return nil
}

func displayCycleInfo(c *maintenance.CycleParams, t time.Time, rep *repo.DirectRepository) {
	printStdout("  scheduled: %v\n", c.Enabled)

	if c.Enabled {
		printStdout("  interval: %v\n", c.Interval)

		if rep.Time().Before(t) {
			printStdout("  next run: %v (in %v)\n", formatTimestamp(t), time.Until(t).Truncate(time.Second))
		} else {
			printStdout("  next run: now\n")
		}
	}
}

func init() {
	maintenanceInfoCommand.Action(directRepositoryAction(runMaintenanceInfoCommand))
}
