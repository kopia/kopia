package policy

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
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
	IntervalSeconds    int64       `json:"intervalSeconds,omitempty"`
	TimesOfDay         []TimeOfDay `json:"timeOfDay,omitempty"`
	NoParentTimesOfDay bool        `json:"noParentTimeOfDay,omitempty"`
	Manual             bool        `json:"manual,omitempty"`
}

// SchedulingPolicyDefinition specifies which policy definition provided the value of a particular field.
type SchedulingPolicyDefinition struct {
	IntervalSeconds snapshot.SourceInfo `json:"intervalSeconds,omitempty"`
	TimesOfDay      snapshot.SourceInfo `json:"timeOfDay,omitempty"`
	Manual          snapshot.SourceInfo `json:"manual,omitempty"`
}

// Interval returns the snapshot interval or zero if not specified.
func (p *SchedulingPolicy) Interval() time.Duration {
	return time.Duration(p.IntervalSeconds) * time.Second
}

// SetInterval sets the snapshot interval (zero disables).
func (p *SchedulingPolicy) SetInterval(d time.Duration) {
	p.IntervalSeconds = int64(d.Seconds())
}

// NextSnapshotTime computes next snapshot time given previous
// snapshot time and current wall clock time.
func (p *SchedulingPolicy) NextSnapshotTime(previousSnapshotTime, now time.Time) (time.Time, bool) {
	if p.Manual {
		return time.Time{}, false
	}

	const oneDay = 24 * time.Hour

	var (
		nextSnapshotTime time.Time
		ok               bool
	)

	now = now.Local()
	previousSnapshotTime = previousSnapshotTime.Local()

	// compute next snapshot time based on interval
	if interval := p.IntervalSeconds; interval != 0 {
		interval := time.Duration(interval) * time.Second

		nt := previousSnapshotTime.Add(interval).Truncate(interval)
		nextSnapshotTime = nt
		ok = true

		if nextSnapshotTime.Before(now) {
			nextSnapshotTime = now
		}
	}

	for _, tod := range p.TimesOfDay {
		nowLocalTime := now.Local()
		localSnapshotTime := time.Date(nowLocalTime.Year(), nowLocalTime.Month(), nowLocalTime.Day(), tod.Hour, tod.Minute, 0, 0, time.Local)

		if now.After(localSnapshotTime) {
			localSnapshotTime = localSnapshotTime.Add(oneDay)
		}

		if !ok || localSnapshotTime.Before(nextSnapshotTime) {
			nextSnapshotTime = localSnapshotTime
			ok = true
		}
	}

	return nextSnapshotTime, ok
}

// Merge applies default values from the provided policy.
func (p *SchedulingPolicy) Merge(src SchedulingPolicy, def *SchedulingPolicyDefinition, si snapshot.SourceInfo) {
	mergeInt64(&p.IntervalSeconds, src.IntervalSeconds, &def.IntervalSeconds, si)

	if len(src.TimesOfDay) > 0 {
		def.TimesOfDay = si

		if !p.NoParentTimesOfDay {
			p.TimesOfDay = SortAndDedupeTimesOfDay(append(append([]TimeOfDay(nil), src.TimesOfDay...), p.TimesOfDay...))
		}
	}

	if src.NoParentTimesOfDay {
		// prevent future merges
		p.NoParentTimesOfDay = src.NoParentTimesOfDay
	}

	mergeBool(&p.Manual, src.Manual, &def.Manual, si)
}

// IsManualSnapshot returns the SchedulingPolicy manual value from the given policy tree.
func IsManualSnapshot(policyTree *Tree) bool {
	return policyTree.EffectivePolicy().SchedulingPolicy.Manual
}

// SetManual sets the manual setting in the SchedulingPolicy on the given source.
func SetManual(ctx context.Context, rep repo.RepositoryWriter, sourceInfo snapshot.SourceInfo) error {
	// Get existing defined policy for the source
	p, err := GetDefinedPolicy(ctx, rep, sourceInfo)

	switch {
	case errors.Is(err, ErrPolicyNotFound):
		p = &Policy{}
	case err != nil:
		return errors.Wrap(err, "could not get defined policy for source")
	}

	p.SchedulingPolicy.Manual = true

	if err := SetPolicy(ctx, rep, sourceInfo, p); err != nil {
		return errors.Wrapf(err, "can't save policy for %v", sourceInfo)
	}

	return nil
}

// ValidateSchedulingPolicy returns an error if manual field is set along with scheduling fields.
func ValidateSchedulingPolicy(p SchedulingPolicy) error {
	if p.Manual && !reflect.DeepEqual(p, SchedulingPolicy{Manual: true}) {
		return errors.New("invalid scheduling policy: manual cannot be combined with other scheduling policies")
	}

	return nil
}
