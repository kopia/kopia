package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
)

var (
	maintenanceSetCommand = maintenanceCommands.Command("set", "Set maintenance parameters")

	maintenanceSetOwner = maintenanceSetCommand.Flag("owner", "Set maintenance owner user@hostname").String()

	maintenanceSetEnableQuick = maintenanceSetCommand.Flag("enable-quick", "Enable or disable quick maintenance").BoolList()
	maintenanceSetEnableFull  = maintenanceSetCommand.Flag("enable-full", "Enable or disable full maintenance").BoolList()

	maintenanceSetQuickFrequency = maintenanceSetCommand.Flag("quick-interval", "Set quick maintenance interval").DurationList()
	maintenanceSetFullFrequency  = maintenanceSetCommand.Flag("full-interval", "Set full maintenance interval").DurationList()

	maintenanceSetPauseQuick = maintenanceSetCommand.Flag("pause-quick", "Pause quick maintenance for a specified duration").DurationList()
	maintenanceSetPauseFull  = maintenanceSetCommand.Flag("pause-full", "Pause full maintenance for a specified duration").DurationList()
)

func setMaintenanceOwnerFromFlags(p *maintenance.Params, rep *repo.DirectRepository, changed *bool) {
	if v := *maintenanceSetOwner; v != "" {
		if v == "me" {
			p.Owner = rep.Username() + "@" + rep.Hostname()
		} else {
			p.Owner = v
		}

		*changed = true

		printStderr("Setting maintenance owner to %v\n", p.Owner)
	}
}

func setMaintenanceEnabledAndIntervalFromFlags(c *maintenance.CycleParams, cycleName string, enableFlag []bool, intervalFlag []time.Duration, changed *bool) {
	// we use lists to distinguish between flag not set
	// Zero elements == not set, more than zero - flag set, in which case we pick the last value
	if len(enableFlag) > 0 {
		lastVal := enableFlag[len(enableFlag)-1]
		c.Enabled = lastVal
		*changed = true

		if lastVal {
			printStderr("Periodic %v maintenance enabled.\n", cycleName)
		} else {
			printStderr("Periodic %v maintenance disabled.\n", cycleName)
		}
	}

	if len(intervalFlag) > 0 {
		lastVal := intervalFlag[len(intervalFlag)-1]
		c.Interval = lastVal
		*changed = true

		printStderr("Interval for %v maintenance set to %v.\n", cycleName, lastVal)
	}
}

func runMaintenanceSetParams(ctx context.Context, rep *repo.DirectRepository) error {
	p, err := maintenance.GetParams(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get current parameters")
	}

	s, err := maintenance.GetSchedule(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to get current parameters")
	}

	var changedParams, changedSchedule bool

	setMaintenanceOwnerFromFlags(p, rep, &changedParams)
	setMaintenanceEnabledAndIntervalFromFlags(&p.QuickCycle, "quick", *maintenanceSetEnableQuick, *maintenanceSetQuickFrequency, &changedParams)
	setMaintenanceEnabledAndIntervalFromFlags(&p.FullCycle, "full", *maintenanceSetEnableFull, *maintenanceSetFullFrequency, &changedParams)

	if v := *maintenanceSetPauseQuick; len(v) > 0 {
		pauseDuration := v[len(v)-1]
		s.NextQuickMaintenanceTime = rep.Time().Add(pauseDuration)
		changedSchedule = true

		printStderr("Quick maintenance paused until %v", formatTimestamp(s.NextQuickMaintenanceTime))
	}

	if v := *maintenanceSetPauseFull; len(v) > 0 {
		pauseDuration := v[len(v)-1]
		s.NextFullMaintenanceTime = rep.Time().Add(pauseDuration)
		changedSchedule = true

		printStderr("Full maintenance paused until %v", formatTimestamp(s.NextFullMaintenanceTime))
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

func init() {
	maintenanceSetCommand.Action(directRepositoryAction(runMaintenanceSetParams))
}
