package policy

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/snapshot"
)

// RetentionPolicy describes snapshot retention policy.
type RetentionPolicy struct {
	KeepLatest  *int `json:"keepLatest,omitempty"`
	KeepHourly  *int `json:"keepHourly,omitempty"`
	KeepDaily   *int `json:"keepDaily,omitempty"`
	KeepWeekly  *int `json:"keepWeekly,omitempty"`
	KeepMonthly *int `json:"keepMonthly,omitempty"`
	KeepAnnual  *int `json:"keepAnnual,omitempty"`
}

// ComputeRetentionReasons computes the reasons why each snapshot is retained, based on
// the settings in retention policy and stores them in RetentionReason field.
func (r *RetentionPolicy) ComputeRetentionReasons(manifests []*snapshot.Manifest) {
	now := time.Now() // allow:no-inject-time
	maxTime := now.Add(365 * 24 * time.Hour)

	cutoffTime := func(setting *int, add func(time.Time, int) time.Time) time.Time {
		if setting != nil {
			return add(now, *setting)
		}

		return maxTime
	}

	cutoff := &cutoffTimes{
		annual:  cutoffTime(r.KeepAnnual, yearsAgo),
		monthly: cutoffTime(r.KeepMonthly, monthsAgo),
		daily:   cutoffTime(r.KeepDaily, daysAgo),
		hourly:  cutoffTime(r.KeepHourly, hoursAgo),
		weekly:  cutoffTime(r.KeepHourly, weeksAgo),
	}

	ids := make(map[string]bool)
	idCounters := make(map[string]int)

	sorted := snapshot.SortByTime(manifests, true)
	for i, s := range sorted {
		s.RetentionReasons = r.getRetentionReasons(i, s, cutoff, ids, idCounters)
	}

	for _, s := range sorted {
		if s.IncompleteReason != "" {
			s.RetentionReasons = append(s.RetentionReasons, "incomplete")
		} else {
			break
		}
	}
}

func (r *RetentionPolicy) getRetentionReasons(i int, s *snapshot.Manifest, cutoff *cutoffTimes, ids map[string]bool, idCounters map[string]int) []string {
	if s.IncompleteReason != "" {
		return nil
	}

	keepReasons := []string{}

	var zeroTime time.Time

	yyyy, wk := s.StartTime.ISOWeek()

	cases := []struct {
		cutoffTime     time.Time
		timePeriodID   string
		timePeriodType string
		max            *int
	}{
		{zeroTime, fmt.Sprintf("%v", i), "latest", r.KeepLatest},
		{cutoff.annual, s.StartTime.Format("2006"), "annual", r.KeepAnnual},
		{cutoff.monthly, s.StartTime.Format("2006-01"), "monthly", r.KeepMonthly},
		{cutoff.weekly, fmt.Sprintf("%04v-%02v", yyyy, wk), "weekly", r.KeepWeekly},
		{cutoff.daily, s.StartTime.Format("2006-01-02"), "daily", r.KeepDaily},
		{cutoff.hourly, s.StartTime.Format("2006-01-02 15"), "hourly", r.KeepHourly},
	}

	for _, c := range cases {
		if c.max == nil {
			continue
		}

		if s.StartTime.Before(c.cutoffTime) {
			continue
		}

		if _, exists := ids[c.timePeriodID]; exists {
			continue
		}

		if idCounters[c.timePeriodType] < *c.max {
			ids[c.timePeriodID] = true
			idCounters[c.timePeriodType]++
			keepReasons = append(keepReasons, fmt.Sprintf("%v-%v", c.timePeriodType, idCounters[c.timePeriodType]))
		}
	}

	return keepReasons
}

type cutoffTimes struct {
	annual  time.Time
	monthly time.Time
	daily   time.Time
	hourly  time.Time
	weekly  time.Time
}

func yearsAgo(base time.Time, n int) time.Time {
	return base.AddDate(-n, 0, 0)
}

func monthsAgo(base time.Time, n int) time.Time {
	return base.AddDate(0, -n, 0)
}

func daysAgo(base time.Time, n int) time.Time {
	return base.AddDate(0, 0, -n)
}

func weeksAgo(base time.Time, n int) time.Time {
	return base.AddDate(0, 0, -n*7) //nolint:gomnd
}

func hoursAgo(base time.Time, n int) time.Time {
	return base.Add(time.Duration(-n) * time.Hour)
}

var defaultRetentionPolicy = RetentionPolicy{
	KeepLatest:  intPtr(10), // nolint:gomnd
	KeepHourly:  intPtr(48), // nolint:gomnd
	KeepDaily:   intPtr(7),  // nolint:gomnd
	KeepWeekly:  intPtr(4),  // nolint:gomnd
	KeepMonthly: intPtr(24), // nolint:gomnd
	KeepAnnual:  intPtr(3),  // nolint:gomnd
}

// Merge applies default values from the provided policy.
func (r *RetentionPolicy) Merge(src RetentionPolicy) {
	if r.KeepLatest == nil {
		r.KeepLatest = src.KeepLatest
	}

	if r.KeepHourly == nil {
		r.KeepHourly = src.KeepHourly
	}

	if r.KeepDaily == nil {
		r.KeepDaily = src.KeepDaily
	}

	if r.KeepWeekly == nil {
		r.KeepWeekly = src.KeepWeekly
	}

	if r.KeepMonthly == nil {
		r.KeepMonthly = src.KeepMonthly
	}

	if r.KeepAnnual == nil {
		r.KeepAnnual = src.KeepAnnual
	}
}
