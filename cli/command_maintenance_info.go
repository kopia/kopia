package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandMaintenanceInfo struct {
	jo  jsonOutput
	out textOutput
}

func (c *commandMaintenanceInfo) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("info", "Display maintenance information").Alias("status")
	c.jo.setup(svc, cmd)
	cmd.Action(svc.directRepositoryReadAction(c.run))
	c.out.setup(svc)
}

func (c *commandMaintenanceInfo) run(ctx context.Context, rep repo.DirectRepository) error {
	p, err := maintenance.GetParams(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get maintenance params")
	}

	s, err := maintenance.GetSchedule(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get maintenance schedule")
	}

	if c.jo.jsonOutput {
		c.out.printStdout("%s\n", c.jo.jsonBytes(s))
		return nil
	}

	c.out.printStdout("Owner: %v\n", p.Owner)
	c.out.printStdout("Quick Cycle:\n")
	c.displayCycleInfo(&p.QuickCycle, s.NextQuickMaintenanceTime, rep)

	c.out.printStdout("Full Cycle:\n")
	c.displayCycleInfo(&p.FullCycle, s.NextFullMaintenanceTime, rep)

	c.out.printStdout("Recent Maintenance Runs:\n")

	for run, timings := range s.Runs {
		c.out.printStdout("  %v:\n", run)

		for _, t := range timings {
			var errInfo string
			if t.Success {
				errInfo = "SUCCESS"
			} else {
				errInfo = "ERROR: " + t.Error
			}

			c.out.printStdout(
				"    %v (%v) %v\n",
				formatTimestamp(t.Start),
				t.End.Sub(t.Start).Truncate(time.Second),
				errInfo)
		}
	}

	return nil
}

func (c *commandMaintenanceInfo) displayCycleInfo(cp *maintenance.CycleParams, t time.Time, rep repo.DirectRepository) {
	c.out.printStdout("  scheduled: %v\n", cp.Enabled)

	if cp.Enabled {
		c.out.printStdout("  interval: %v\n", cp.Interval)

		if rep.Time().Before(t) {
			c.out.printStdout("  next run: %v (in %v)\n", formatTimestamp(t), clock.Until(t).Truncate(time.Second))
		} else {
			c.out.printStdout("  next run: now\n")
		}
	}
}
