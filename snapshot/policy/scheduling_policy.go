package policy

import (
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
)

// TimeOfDay represents the time of day (hh:mm) using 24-hour time format.
type TimeOfDay struct {
	Hour   int `json:"hour"`
	Minute int `json:"min"`
}

// Parse parses the time of day.
func (t *TimeOfDay) Parse(s string) error {
	if _, err := fmt.Sscanf(s, "%v:%02v", &t.Hour, &t.Minute); err != nil {
		return errors.New("invalid time of day, must be HH:MM")
	}

	if t.Hour < 0 || t.Hour > 23 {
		return errors.Errorf("invalid hour %q, must be between 0 and 23", s)
	}

	if t.Minute < 0 || t.Minute > 59 {
		return errors.Errorf("invalid minute %q, must be between 0 and 59", s)
	}

	return nil
}

// String returns string representation of time of day.
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
	IntervalSeconds int64       `json:"intervalSeconds,omitempty"`
	TimesOfDay      []TimeOfDay `json:"timeOfDay,omitempty"`
}

// Interval returns the snapshot interval or zero if not specified.
func (p *SchedulingPolicy) Interval() time.Duration {
	return time.Duration(p.IntervalSeconds) * time.Second
}

// SetInterval sets the snapshot interval (zero disables).
func (p *SchedulingPolicy) SetInterval(d time.Duration) {
	p.IntervalSeconds = int64(d.Seconds())
}

// Merge applies default values from the provided policy.
func (p *SchedulingPolicy) Merge(src SchedulingPolicy) {
	if p.IntervalSeconds == 0 {
		p.IntervalSeconds = src.IntervalSeconds
	}

	p.TimesOfDay = SortAndDedupeTimesOfDay(
		append(append([]TimeOfDay(nil), src.TimesOfDay...), p.TimesOfDay...))
}

var defaultSchedulingPolicy = SchedulingPolicy{}
