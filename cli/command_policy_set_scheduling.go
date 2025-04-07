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
	policySetCron       string
	policySetManual     bool
	policySetRunMissed  string
}

func (c *policySchedulingFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("snapshot-interval", "Interval between snapshots").DurationListVar(&c.policySetInterval)
	cmd.Flag("snapshot-time", "Comma-separated times of day when to take snapshot (HH:mm,HH:mm,...) or 'inherit' to remove override").StringsVar(&c.policySetTimesOfDay)
	cmd.Flag("snapshot-time-crontab", "Semicolon-separated crontab-compatible expressions (or 'inherit')").StringVar(&c.policySetCron)
	cmd.Flag("run-missed", "Run missed time-of-day or cron snapshots ('true', 'false', 'inherit')").EnumVar(&c.policySetRunMissed, booleanEnumValues...)
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
			log(ctx).Info(" - resetting snapshot times of day to default")
		} else {
			log(ctx).Infof(" - setting snapshot times to %v", timesOfDay)
		}
	}

	if c.policySetCron != "" {
		ce := splitCronExpressions(c.policySetCron)

		if ce == nil {
			log(ctx).Info(" - resetting cron snapshot times to default")
		} else {
			log(ctx).Infof(" - setting cron snapshot times to %v", ce)
		}

		*changeCount++

		sp.Cron = ce

		if err := policy.ValidateSchedulingPolicy(*sp); err != nil {
			return errors.Wrap(err, "invalid scheduling policy")
		}
	}

	if err := c.setRunMissedFromFlags(ctx, sp, changeCount); err != nil {
		return errors.Wrap(err, "invalid run-missed value")
	}

	if sp.Manual {
		*changeCount++

		sp.Manual = false

		log(ctx).Info(" - resetting manual snapshot field to false\n")
	}

	return nil
}

// Update RunMissed policy flag if changed.
func (c *policySchedulingFlags) setRunMissedFromFlags(ctx context.Context, sp *policy.SchedulingPolicy, changeCount *int) error {
	if err := applyPolicyBoolPtr(ctx, "run missed snapshots", &sp.RunMissed, c.policySetRunMissed, changeCount); err != nil {
		return errors.Wrap(err, "invalid scheduling policy")
	}

	return nil
}

// splitCronExpressions splits the provided string into a list of cron expressions.
// Individual items are separated by semi-colons. As a special case, the string "inherit"
// returns a nil slice.
func splitCronExpressions(expr string) []string {
	if expr == inheritPolicyString || expr == defaultPolicyString {
		return nil
	}

	var result []string

	parts := strings.Split(expr, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		result = append(result, part)
	}

	return result
}

func (c *policySchedulingFlags) setManualFromFlags(ctx context.Context, sp *policy.SchedulingPolicy, changeCount *int) error {
	// Cannot set both schedule and manual setting
	if len(c.policySetInterval) > 0 || len(c.policySetTimesOfDay) > 0 || c.policySetCron != "" {
		return errors.New("cannot set manual field when scheduling snapshots")
	}

	// Reset the existing policy schedule, if present
	if sp.IntervalSeconds != 0 {
		*changeCount++

		sp.IntervalSeconds = 0

		log(ctx).Info(" - resetting snapshot interval to default\n")
	}

	if len(sp.TimesOfDay) > 0 {
		*changeCount++

		sp.TimesOfDay = nil

		log(ctx).Info(" - resetting snapshot times of day to default\n")
	}

	if len(sp.Cron) > 0 {
		*changeCount++

		sp.Cron = nil

		log(ctx).Info(" - resetting cron snapshot times to default\n")
	}

	*changeCount++

	sp.Manual = c.policySetManual
	log(ctx).Infof(" - setting manual snapshot field to %v\n", c.policySetManual)

	return nil
}
