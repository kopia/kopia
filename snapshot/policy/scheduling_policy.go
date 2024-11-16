package policy

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/cronexpr"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

// TimeOfDay represents the time of day (hh:mm) using 24-hour time format.
//
//nolint:recvcheck
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
	slices.SortFunc(tod, func(a, b TimeOfDay) int {
		if n := cmp.Compare(a.Hour, b.Hour); n != 0 {
			return n
		}

		// If hours are equal sort by minute
		return cmp.Compare(a.Minute, b.Minute)
	})

	// Remove subsequent duplicates
	return slices.Compact[[]TimeOfDay, TimeOfDay](tod)
}

// SchedulingPolicy describes policy for scheduling snapshots.
type SchedulingPolicy struct {
	IntervalSeconds    int64         `json:"intervalSeconds,omitempty"`
	TimesOfDay         []TimeOfDay   `json:"timeOfDay,omitempty"`
	NoParentTimesOfDay bool          `json:"noParentTimeOfDay,omitempty"`
	Manual             bool          `json:"manual,omitempty"`
	Cron               []string      `json:"cron,omitempty"`
	RunMissed          *OptionalBool `json:"runMissed,omitempty"`
}

// SchedulingPolicyDefinition specifies which policy definition provided the value of a particular field.
type SchedulingPolicyDefinition struct {
	IntervalSeconds snapshot.SourceInfo `json:"intervalSeconds,omitempty"`
	TimesOfDay      snapshot.SourceInfo `json:"timeOfDay,omitempty"`
	Cron            snapshot.SourceInfo `json:"cron,omitempty"`
	Manual          snapshot.SourceInfo `json:"manual,omitempty"`
	RunMissed       snapshot.SourceInfo `json:"runMissed,omitempty"`
}

// defaultRunMissed is the value for RunMissed.
const defaultRunMissed = true

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

	if todSnapshot, todOk := p.getNextTimeOfDaySnapshot(now); todOk && (!ok || todSnapshot.Before(nextSnapshotTime)) {
		nextSnapshotTime = todSnapshot
		ok = true
	}

	if cronSnapshot, cronOk := p.getNextCronSnapshot(now); cronOk && (!ok || cronSnapshot.Before(nextSnapshotTime)) {
		nextSnapshotTime = cronSnapshot
		ok = true
	}

	if ok && p.checkMissedSnapshot(now, previousSnapshotTime, nextSnapshotTime) {
		// if RunMissed is set and last run was missed, and next run is at least 30 mins from now, then run now
		nextSnapshotTime = now
		ok = true
	}

	return nextSnapshotTime, ok
}

// Get next ToD snapshot.
func (p *SchedulingPolicy) getNextTimeOfDaySnapshot(now time.Time) (time.Time, bool) {
	const oneDay = 24 * time.Hour

	var nextSnapshotTime time.Time

	ok := false
	nowLocalTime := now.Local()

	for _, tod := range p.TimesOfDay {
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

// Get next Cron snapshot.
func (p *SchedulingPolicy) getNextCronSnapshot(now time.Time) (time.Time, bool) {
	var nextSnapshotTime time.Time

	ok := false

	for _, e := range p.Cron {
		ce, err := cronexpr.Parse(stripCronComment(e))
		if err != nil {
			// ignore invalid crontab entries, nothing we can do at this point
			// we already validated cron expressions them when they were added to the policy.
			continue
		}

		nt := ce.Next(now)
		if nt.IsZero() {
			continue
		}

		if !ok || nt.Before(nextSnapshotTime) {
			nextSnapshotTime = nt
			ok = true
		}
	}

	return nextSnapshotTime, ok
}

// Check if a previous snapshot was missed and should be started now.
func (p *SchedulingPolicy) checkMissedSnapshot(now, previousSnapshotTime, nextSnapshotTime time.Time) bool {
	const halfhour = 30 * time.Minute

	momentAfterSnapshot := previousSnapshotTime.Add(time.Second)

	if !p.RunMissed.OrDefault(false) {
		return false
	}

	nextSnapshot := nextSnapshotTime
	// We add a second to ensure that the next possible snapshot is > the last snaphot
	todSnapshot, todOk := p.getNextTimeOfDaySnapshot(momentAfterSnapshot)
	cronSnapshot, cronOk := p.getNextCronSnapshot(momentAfterSnapshot)

	if !todOk && !cronOk {
		return false
	}

	if todOk && todSnapshot.Before(nextSnapshot) {
		nextSnapshot = todSnapshot
	}

	if cronOk && cronSnapshot.Before(nextSnapshot) {
		nextSnapshot = cronSnapshot
	}

	return nextSnapshot.Before(now) && nextSnapshotTime.After(now.Add(halfhour))
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

	mergeStringList(&p.Cron, src.Cron, &def.Cron, si)

	if src.NoParentTimesOfDay {
		// prevent future merges
		p.NoParentTimesOfDay = src.NoParentTimesOfDay
	}

	mergeBool(&p.Manual, src.Manual, &def.Manual, si)
	mergeOptionalBool(&p.RunMissed, src.RunMissed, &def.RunMissed, si)
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

	for _, e := range p.Cron {
		if e2 := stripCronComment(e); e2 != "" {
			if _, err := cronexpr.Parse(e2); err != nil {
				return errors.Errorf("invalid cron expression %q", e)
			}
		}
	}

	return nil
}

func stripCronComment(s string) string {
	return strings.TrimSpace(strings.SplitN(s, "#", 2)[0]) //nolint:mnd
}
