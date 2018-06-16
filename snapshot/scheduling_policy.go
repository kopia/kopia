package snapshot

import (
	"fmt"
	"sort"
	"time"
)

// TimeOfDay represents the time of day (hh:mm) using 24-hour time format.
type TimeOfDay struct {
	Hour   int `json:"hour"`
	Minute int `json:"min"`
}

// Parse parses the time of day.
func (t *TimeOfDay) Parse(s string) error {
	if _, err := fmt.Sscanf(s, "%v:%02v", &t.Hour, &t.Minute); err != nil {
		return fmt.Errorf("invalid time of day, must be HH:MM")
	}
	if t.Hour < 0 || t.Hour > 23 {
		return fmt.Errorf("invalid hour %q, must be between 0 and 23", s)
	}
	if t.Minute < 0 || t.Minute > 59 {
		return fmt.Errorf("invalid minute %q, must be between 0 and 59", s)
	}

	return nil
}

// TimeOfDay returns string representation of time of day.
func (t TimeOfDay) String() string {
	return fmt.Sprintf("%v:%02v", t.Hour, t.Minute)
}

// SortAndDedupeTimesOfDay sorts the slice of times of day and removes duplicates.
func SortAndDedupeTimesOfDay(tod []TimeOfDay) []TimeOfDay {
	sort.Slice(tod, func(i, j int) bool {
		if a, b := tod[i].Hour, tod[j].Hour; a != b {
			return a < b
		}
		return tod[i].Minute < tod[j].Minute
	})

	return tod
}

// SchedulingPolicy describes policy for scheduling snapshots.
type SchedulingPolicy struct {
	Interval   *time.Duration `json:"interval"`
	TimesOfDay []TimeOfDay    `json:"timeOfDay"`
}

func mergeSchedulingPolicy(dst, src *SchedulingPolicy) {
	if dst.Interval == nil {
		dst.Interval = src.Interval
	}
	dst.TimesOfDay = SortAndDedupeTimesOfDay(
		append(append([]TimeOfDay(nil), src.TimesOfDay...), dst.TimesOfDay...))
}

var defaultSchedulingPolicy = &SchedulingPolicy{}
