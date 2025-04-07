package policy

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kopia/kopia/snapshot"
)

const (
	// keep all snapshots younger than this.
	retainIncompleteSnapshotsYoungerThan = 4 * time.Hour

	// minimal number of incomplete snapshots to keep.
	retainIncompleteSnapshotMinimumCount = 3
)

// RetentionPolicy describes snapshot retention policy.
type RetentionPolicy struct {
	KeepLatest               *OptionalInt  `json:"keepLatest,omitempty"`
	KeepHourly               *OptionalInt  `json:"keepHourly,omitempty"`
	KeepDaily                *OptionalInt  `json:"keepDaily,omitempty"`
	KeepWeekly               *OptionalInt  `json:"keepWeekly,omitempty"`
	KeepMonthly              *OptionalInt  `json:"keepMonthly,omitempty"`
	KeepAnnual               *OptionalInt  `json:"keepAnnual,omitempty"`
	IgnoreIdenticalSnapshots *OptionalBool `json:"ignoreIdenticalSnapshots,omitempty"`
}

// RetentionPolicyDefinition specifies which policy definition provided the value of a particular field.
type RetentionPolicyDefinition struct {
	KeepLatest               snapshot.SourceInfo `json:"keepLatest,omitempty"`
	KeepHourly               snapshot.SourceInfo `json:"keepHourly,omitempty"`
	KeepDaily                snapshot.SourceInfo `json:"keepDaily,omitempty"`
	KeepWeekly               snapshot.SourceInfo `json:"keepWeekly,omitempty"`
	KeepMonthly              snapshot.SourceInfo `json:"keepMonthly,omitempty"`
	KeepAnnual               snapshot.SourceInfo `json:"keepAnnual,omitempty"`
	IgnoreIdenticalSnapshots snapshot.SourceInfo `json:"ignoreIdenticalSnapshots,omitempty"`
}

// ComputeRetentionReasons computes the reasons why each snapshot is retained, based on
// the settings in retention policy and stores them in RetentionReason field.
func (r *RetentionPolicy) ComputeRetentionReasons(manifests []*snapshot.Manifest) {
	if len(manifests) == 0 {
		return
	}

	// compute max time across all and complete snapshots
	var (
		maxCompleteStartTime time.Time
		maxStartTime         time.Time
	)

	for _, m := range manifests {
		if m.StartTime.ToTime().After(maxStartTime) {
			maxStartTime = m.StartTime.ToTime()
		}

		if m.IncompleteReason == "" && m.StartTime.ToTime().After(maxCompleteStartTime) {
			maxCompleteStartTime = m.StartTime.ToTime()
		}
	}

	maxTime := maxCompleteStartTime.Add(365 * 24 * time.Hour)

	cutoffTime := func(setting *OptionalInt, add func(time.Time, int) time.Time) time.Time {
		if setting != nil {
			return add(maxCompleteStartTime, int(*setting))
		}

		return maxTime
	}

	cutoff := &cutoffTimes{
		annual:  cutoffTime(r.KeepAnnual, yearsAgo),
		monthly: cutoffTime(r.KeepMonthly, monthsAgo),
		daily:   cutoffTime(r.KeepDaily, daysAgo),
		hourly:  cutoffTime(r.KeepHourly, hoursAgo),
		weekly:  cutoffTime(r.KeepWeekly, weeksAgo),
	}

	ids := make(map[string]bool)
	idCounters := make(map[string]int)

	// sort manifests in descending time order (most recent first)
	sorted := snapshot.SortByTime(manifests, true)

	// apply retention reasons to complete snapshots
	for i, s := range sorted {
		if s.IncompleteReason == "" {
			s.RetentionReasons = r.getRetentionReasons(i, s, cutoff, ids, idCounters)
		} else {
			s.RetentionReasons = []string{}
		}
	}

	// attach 'retention reason' tag to incomplete snapshots until we run into first complete one
	// or we have enough incomplete ones and we run into an old one.
	for i, s := range sorted {
		if s.IncompleteReason == "" {
			break
		}

		age := maxStartTime.Sub(s.StartTime.ToTime())
		// retain incomplete snapshots below certain age and below maximum count.
		if age < retainIncompleteSnapshotsYoungerThan || i < retainIncompleteSnapshotMinimumCount {
			s.RetentionReasons = append(s.RetentionReasons, "incomplete")
		} else {
			break
		}
	}
}

// EffectiveKeepLatest returns the number of "latest" snapshots to keep. If all
// retention values are set to 0 then returns MaxInt.
func (r *RetentionPolicy) EffectiveKeepLatest() *OptionalInt {
	if r.KeepLatest.OrDefault(0)+r.KeepHourly.OrDefault(0)+r.KeepDaily.OrDefault(0)+r.KeepWeekly.OrDefault(0)+r.KeepMonthly.OrDefault(0)+r.KeepAnnual.OrDefault(0) == 0 {
		return newOptionalInt(math.MaxInt)
	}

	return r.KeepLatest
}

func (r *RetentionPolicy) getRetentionReasons(i int, s *snapshot.Manifest, cutoff *cutoffTimes, ids map[string]bool, idCounters map[string]int) []string {
	if s.IncompleteReason != "" {
		return nil
	}

	keepReasons := []string{}

	var zeroTime time.Time

	yyyy, wk := s.StartTime.ToTime().ISOWeek()

	effectiveKeepLatest := r.EffectiveKeepLatest()

	cases := []struct {
		cutoffTime     time.Time
		timePeriodID   string
		timePeriodType string
		max            *OptionalInt
	}{
		{zeroTime, strconv.Itoa(i), "latest", effectiveKeepLatest},
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

		if s.StartTime.ToTime().Before(c.cutoffTime) {
			continue
		}

		if _, exists := ids[c.timePeriodID]; exists {
			continue
		}

		if idCounters[c.timePeriodType] < int(*c.max) {
			ids[c.timePeriodID] = true
			idCounters[c.timePeriodType]++
			keepReasons = append(keepReasons, fmt.Sprintf("%v-%v", c.timePeriodType, idCounters[c.timePeriodType]))
		}
	}

	SortRetentionTags(keepReasons)

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
	return base.AddDate(0, 0, -n*7) //nolint:mnd
}

func hoursAgo(base time.Time, n int) time.Time {
	return base.Add(time.Duration(-n) * time.Hour)
}

const (
	defaultKeepLatest               = 10
	defaultKeepHourly               = 48
	defaultKeepDaily                = 7
	defaultKeepWeekly               = 4
	defaultKeepMonthly              = 24
	defaultKeepAnnual               = 3
	defaultIgnoreIdenticalSnapshots = false
)

// Merge applies default values from the provided policy.
func (r *RetentionPolicy) Merge(src RetentionPolicy, def *RetentionPolicyDefinition, si snapshot.SourceInfo) {
	mergeOptionalInt(&r.KeepLatest, src.KeepLatest, &def.KeepLatest, si)
	mergeOptionalInt(&r.KeepHourly, src.KeepHourly, &def.KeepHourly, si)
	mergeOptionalInt(&r.KeepDaily, src.KeepDaily, &def.KeepDaily, si)
	mergeOptionalInt(&r.KeepWeekly, src.KeepWeekly, &def.KeepWeekly, si)
	mergeOptionalInt(&r.KeepMonthly, src.KeepMonthly, &def.KeepMonthly, si)
	mergeOptionalInt(&r.KeepAnnual, src.KeepAnnual, &def.KeepAnnual, si)
	mergeOptionalBool(&r.IgnoreIdenticalSnapshots, src.IgnoreIdenticalSnapshots, &def.IgnoreIdenticalSnapshots, si)
}

// CompactRetentionReasons returns compressed retention reasons given a list of retention reasons.
func CompactRetentionReasons(reasons []string) []string {
	reasonsByPrefix := map[string][]int{}

	result := []string{}

	for _, r := range reasons {
		prefix, suffix := prefixSuffix(r)

		n, err := strconv.Atoi(suffix)
		if err != nil {
			result = append(result, r)
			continue
		}

		reasonsByPrefix[prefix] = append(reasonsByPrefix[prefix], n)
	}

	for prefix, v := range reasonsByPrefix {
		result = appendRLE(result, prefix, v)
	}

	SortRetentionTags(result)

	return result
}

func prefixSuffix(s string) (prefix, suffix string) {
	if p := strings.LastIndex(s, "-"); p < 0 {
		prefix = s
		suffix = ""
	} else {
		prefix = s[0:p]
		suffix = s[p+1:]
	}

	return
}

func appendRLE(out []string, prefix string, numbers []int) []string {
	sort.Ints(numbers)

	runStart := numbers[0]
	runEnd := numbers[0]

	appendRun := func() {
		if runStart == runEnd {
			out = append(out, fmt.Sprintf("%v-%v", prefix, runStart))
		} else {
			out = append(out, fmt.Sprintf("%v-%v..%v", prefix, runStart, runEnd))
		}
	}

	for _, num := range numbers[1:] {
		if num == runEnd+1 {
			runEnd = num
		} else {
			appendRun()

			runStart = num
			runEnd = runStart
		}
	}

	appendRun()

	return out
}

// CompactPins returns compressed pins reasons given a list of pins.
func CompactPins(pins []string) []string {
	cnt := map[string]int{}

	for _, p := range pins {
		cnt[p]++
	}

	result := []string{}

	for k := range cnt {
		result = append(result, k)
	}

	sort.Strings(result)

	return result
}

// SortRetentionTags sorts the provided retention tags in canonical order.
func SortRetentionTags(tags []string) {
	retentionPrefixSortValue := map[string]int{
		"latest":  1,
		"hourly":  2, //nolint:mnd
		"daily":   3, //nolint:mnd
		"weekly":  4, //nolint:mnd
		"monthly": 5, //nolint:mnd
		"annual":  6, //nolint:mnd
	}

	sort.Slice(tags, func(i, j int) bool {
		p1, s1 := prefixSuffix(tags[i])
		p2, s2 := prefixSuffix(tags[j])

		if l, r := retentionPrefixSortValue[p1], retentionPrefixSortValue[p2]; l != r {
			return l < r
		}

		if l, r := p1, p2; l != r {
			return p1 < p2
		}

		return s1 < s2
	})
}
