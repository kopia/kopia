package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

type commandMaintenanceSet struct {
	maintenanceSetOwner          string
	maintenanceSetEnableQuick    []bool          // optional boolean
	maintenanceSetEnableFull     []bool          // optional boolean
	maintenanceSetQuickFrequency []time.Duration // optional duration
	maintenanceSetFullFrequency  []time.Duration // optional duration
	maintenanceSetPauseQuick     []time.Duration // optional duration
	maintenanceSetPauseFull      []time.Duration // optional duration
}

func (c *commandMaintenanceSet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Set maintenance parameters")

	cmd.Flag("owner", "Set maintenance owner user@hostname").StringVar(&c.maintenanceSetOwner)

	cmd.Flag("enable-quick", "Enable or disable quick maintenance").BoolListVar(&c.maintenanceSetEnableQuick)
	cmd.Flag("enable-full", "Enable or disable full maintenance").BoolListVar(&c.maintenanceSetEnableFull)

	cmd.Flag("quick-interval", "Set quick maintenance interval").DurationListVar(&c.maintenanceSetQuickFrequency)
	cmd.Flag("full-interval", "Set full maintenance interval").DurationListVar(&c.maintenanceSetFullFrequency)

	cmd.Flag("pause-quick", "Pause quick maintenance for a specified duration").DurationListVar(&c.maintenanceSetPauseQuick)
	cmd.Flag("pause-full", "Pause full maintenance for a specified duration").DurationListVar(&c.maintenanceSetPauseFull)

	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandMaintenanceSet) setMaintenanceOwnerFromFlags(ctx context.Context, p *maintenance.Params, rep repo.DirectRepositoryWriter, changed *bool) {
	if v := c.maintenanceSetOwner; v != "" {
		if v == "me" {
			p.Owner = rep.ClientOptions().UsernameAtHost()
		} else {
			p.Owner = v
		}

		*changed = true

		log(ctx).Infof("Setting maintenance owner to %v", p.Owner)
	}
}

func (c *commandMaintenanceSet) setMaintenanceEnabledAndIntervalFromFlags(ctx context.Context, cp *maintenance.CycleParams, cycleName string, enableFlag []bool, intervalFlag []time.Duration, changed *bool) {
	// we use lists to distinguish between flag not set
	// Zero elements == not set, more than zero - flag set, in which case we pick the last value
	if len(enableFlag) > 0 {
		lastVal := enableFlag[len(enableFlag)-1]
		cp.Enabled = lastVal
		*changed = true

		if lastVal {
			log(ctx).Infof("Periodic %v maintenance enabled.", cycleName)
		} else {
			log(ctx).Infof("Periodic %v maintenance disabled.", cycleName)
		}
	}

	if len(intervalFlag) > 0 {
		lastVal := intervalFlag[len(intervalFlag)-1]
		cp.Interval = lastVal
		*changed = true

		log(ctx).Infof("Interval for %v maintenance set to %v.", cycleName, lastVal)
	}
}

func (c *commandMaintenanceSet) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	p, err := maintenance.GetParams(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get current parameters")
	}

	s, err := maintenance.GetSchedule(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get current parameters")
	}

	var changedParams, changedSchedule bool

	c.setMaintenanceOwnerFromFlags(ctx, p, rep, &changedParams)
	c.setMaintenanceEnabledAndIntervalFromFlags(ctx, &p.QuickCycle, "quick", c.maintenanceSetEnableQuick, c.maintenanceSetQuickFrequency, &changedParams)
	c.setMaintenanceEnabledAndIntervalFromFlags(ctx, &p.FullCycle, "full", c.maintenanceSetEnableFull, c.maintenanceSetFullFrequency, &changedParams)

	if v := c.maintenanceSetPauseQuick; len(v) > 0 {
		pauseDuration := v[len(v)-1]
		s.NextQuickMaintenanceTime = rep.Time().Add(pauseDuration)
		changedSchedule = true

		log(ctx).Infof("Quick maintenance paused until %v", formatTimestamp(s.NextQuickMaintenanceTime))
	}

	if v := c.maintenanceSetPauseFull; len(v) > 0 {
		pauseDuration := v[len(v)-1]
		s.NextFullMaintenanceTime = rep.Time().Add(pauseDuration)
		changedSchedule = true

		log(ctx).Infof("Full maintenance paused until %v", formatTimestamp(s.NextFullMaintenanceTime))
	}

	if !changedParams && !changedSchedule {
		return errors.Errorf("no changes specified")
	}

	if changedSchedule {
		if err := maintenance.SetSchedule(ctx, rep, s); err != nil {
			return errors.Wrap(err, "unable to set schedule")
		}
	}

	if changedParams {
		if err := maintenance.SetParams(ctx, rep, p); err != nil {
			return errors.Wrap(err, "unable to set params")
		}
	}

	return nil
}
