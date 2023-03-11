package cli

import (
	"context"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

type policySchedulingFlags struct {
	policySetInterval   []time.Duration // not a list, just optional duration
	policySetTimesOfDay []string
	policySetManual     bool
}

func (c *policySchedulingFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("snapshot-interval", "Interval between snapshots").DurationListVar(&c.policySetInterval)
	cmd.Flag("snapshot-time", "Comma-separated times of day when to take snapshot (HH:mm,HH:mm,...) or 'inherit' to remove override").StringsVar(&c.policySetTimesOfDay)
	cmd.Flag("manual", "Only create snapshots manually").BoolVar(&c.policySetManual)
}

func (c *policySchedulingFlags) setSchedulingPolicyFromFlags(ctx context.Context, sp *policy.SchedulingPolicy, changeCount *int) error {
	if c.policySetManual {
		return c.setManualFromFlags(ctx, sp, changeCount)
	}

	return c.setScheduleFromFlags(ctx, sp, changeCount)
}

func (c *policySchedulingFlags) setScheduleFromFlags(ctx context.Context, sp *policy.SchedulingPolicy, changeCount *int) error {
	// It's not really a list, just optional value.
	for _, interval := range c.policySetInterval {
		*changeCount++

		sp.SetInterval(interval)
		log(ctx).Infof(" - setting snapshot interval to %v", sp.Interval())

		break
	}

	if len(c.policySetTimesOfDay) > 0 {
		var timesOfDay []policy.TimeOfDay

		for _, tods := range c.policySetTimesOfDay {
			for _, tod := range strings.Split(tods, ",") {
				if tod == inheritPolicyString {
					timesOfDay = nil
					break
				}

				var timeOfDay policy.TimeOfDay
				if err := timeOfDay.Parse(tod); err != nil {
					return errors.Wrap(err, "unable to parse time of day")
				}

				timesOfDay = append(timesOfDay, timeOfDay)
			}
		}
		*changeCount++

		sp.TimesOfDay = policy.SortAndDedupeTimesOfDay(timesOfDay)

		if timesOfDay == nil {
			log(ctx).Infof(" - resetting snapshot times of day to default")
		} else {
			log(ctx).Infof(" - setting snapshot times to %v", timesOfDay)
		}
	}

	if sp.Manual {
		*changeCount++

		sp.Manual = false

		log(ctx).Infof(" - resetting manual snapshot field to false\n")
	}

	return nil
}

func (c *policySchedulingFlags) setManualFromFlags(ctx context.Context, sp *policy.SchedulingPolicy, changeCount *int) error {
	// Cannot set both schedule and manual setting
	if len(c.policySetInterval) > 0 || len(c.policySetTimesOfDay) > 0 {
		return errors.New("cannot set manual field when scheduling snapshots")
	}

	// Reset the existing policy schedule, if present
	if sp.IntervalSeconds != 0 {
		*changeCount++

		sp.IntervalSeconds = 0

		log(ctx).Infof(" - resetting snapshot interval to default\n")
	}

	if len(sp.TimesOfDay) > 0 {
		*changeCount++

		sp.TimesOfDay = nil

		log(ctx).Infof(" - resetting snapshot times of day to default\n")
	}

	*changeCount++

	sp.Manual = c.policySetManual
	log(ctx).Infof(" - setting manual snapshot field to %v\n", c.policySetManual)

	return nil
}
