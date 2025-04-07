package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandMaintenanceInfo struct {
	jo  jsonOutput
	out textOutput
}

// MaintenanceInfo is used to display the maintenance info in JSON format.
type MaintenanceInfo struct {
	maintenance.Params
	maintenance.Schedule `json:"schedule"`
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
		mi := MaintenanceInfo{
			Params:   *p,
			Schedule: *s,
		}

		c.out.printStdout("%s\n", c.jo.jsonBytes(mi))

		return nil
	}

	c.out.printStdout("Owner: %v\n", p.Owner)
	c.out.printStdout("Quick Cycle:\n")
	c.displayCycleInfo(&p.QuickCycle, s.NextQuickMaintenanceTime, rep)

	c.out.printStdout("Full Cycle:\n")
	c.displayCycleInfo(&p.FullCycle, s.NextFullMaintenanceTime, rep)

	cl := p.LogRetention.OrDefault()

	c.out.printStdout("Log Retention:\n")
	c.out.printStdout("  max count:       %v\n", cl.MaxCount)
	c.out.printStdout("  max age of logs: %v\n", cl.MaxAge)
	c.out.printStdout("  max total size:  %v\n", units.BytesString(cl.MaxTotalSize))

	if p.ExtendObjectLocks {
		c.out.printStdout("Object Lock Extension: enabled\n")
	} else {
		c.out.printStdout("Object Lock Extension: disabled\n")
	}

	if p.ListParallelism != 0 {
		c.out.printStdout("List parallelism: %v\n", p.ListParallelism)
	}

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
			c.out.printStdout("  next run: %v (in %v)\n", formatTimestamp(t), t.Sub(clock.Now()).Truncate(time.Second))
		} else {
			c.out.printStdout("  next run: now\n")
		}
	}
}
