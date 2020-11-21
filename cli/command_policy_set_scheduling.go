package cli

import (
	"context"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

var (
	// Frequency.
	policySetInterval   = policySetCommand.Flag("snapshot-interval", "Interval between snapshots").DurationList()
	policySetTimesOfDay = policySetCommand.Flag("snapshot-time", "Times of day when to take snapshot (HH:mm)").Strings()
)

func setSchedulingPolicyFromFlags(ctx context.Context, sp *policy.SchedulingPolicy, changeCount *int) error {
	// It's not really a list, just optional value.
	for _, interval := range *policySetInterval {
		*changeCount++

		sp.SetInterval(interval)
		log(ctx).Infof(" - setting snapshot interval to %v\n", sp.Interval())

		break
	}

	if len(*policySetTimesOfDay) > 0 {
		var timesOfDay []policy.TimeOfDay

		for _, tods := range *policySetTimesOfDay {
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
			log(ctx).Infof(" - resetting snapshot times of day to default\n")
		} else {
			log(ctx).Infof(" - setting snapshot times to %v\n", timesOfDay)
		}
	}

	return nil
}
