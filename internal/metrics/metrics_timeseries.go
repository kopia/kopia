package metrics

import (
	"context"
	"sort"
	"time"
)

// TimeSeries represents a time series of a counter or a distribution.
type TimeSeries[T any] []TimeSeriesPoint[T]

// TimeSeriesPoint represents a single data point in a time series.
type TimeSeriesPoint[T any] struct {
	Time  time.Time
	Value T
}

// AggregateByFunc is a function that aggregates a given username
// and hostname into a single string representing final time series ID.
type AggregateByFunc func(username, hostname string) string

// AggregateByUser is an aggregation function that aggregates by user@hostname.
func AggregateByUser(username, hostname string) string {
	return username + "@" + hostname
}

// AggregateByHost is an aggregation function that aggregates by hostname.
//
//nolint:revive
func AggregateByHost(username, hostname string) string {
	return hostname
}

// AggregateAll is an aggregation function that aggregates all data into a single series.
//
//nolint:revive
func AggregateAll(username, hostname string) string {
	return "*"
}

// AggregateMetricsOptions represents options for AggregateCounter function.
type AggregateMetricsOptions struct {
	TimeResolution TimeResolutionFunc
	AggregateBy    AggregateByFunc
}

// SnapshotValueAggregator extracts and aggregates counter or distribution values from snapshots.
type SnapshotValueAggregator[T any] interface {
	FromSnapshot(s *Snapshot) (T, bool)
	Aggregate(previousAggregate T, incoming T, ratio float64) T
}

// CreateTimeSeries computes time series which represent aggregations of a given
// counters or distributions over a set of snapshots.
func CreateTimeSeries[TValue any](
	ctx context.Context,
	snapshots []*Snapshot,
	valueHandler SnapshotValueAggregator[TValue],
	opts AggregateMetricsOptions,
) map[string]TimeSeries[TValue] {
	ts := map[string]map[time.Time]TValue{}

	if opts.AggregateBy == nil {
		opts.AggregateBy = AggregateByUser
	}

	if opts.TimeResolution == nil {
		opts.TimeResolution = TimeResolutionByDay
	}

	var minTime, maxTime time.Time

	// generate time series for the specified aggregation
	for _, s := range snapshots {
		value, ok := valueHandler.FromSnapshot(s)
		if !ok {
			continue
		}

		timeSeriesID := opts.AggregateBy(s.User, s.Hostname)

		if _, ok := ts[timeSeriesID]; !ok {
			ts[timeSeriesID] = map[time.Time]TValue{}
		}

		firstPoint, _ := opts.TimeResolution(s.StartTime)
		_, lastPoint := opts.TimeResolution(s.EndTime)

		if minTime.IsZero() || firstPoint.Before(minTime) {
			minTime = firstPoint
		}

		if lastPoint.After(maxTime) {
			maxTime = lastPoint
		}

		totalDuration := s.EndTime.Sub(s.StartTime)
		pbt := ts[timeSeriesID]

		// we know that between [StartTime, EndTime] the counter increased by `value`
		// distribute counter value among points in the time series proportionally to the
		// time spent in time period
		for p := s.StartTime; p.Before(s.EndTime); _, p = opts.TimeResolution(p) {
			point, next := opts.TimeResolution(p)
			if next.After(s.EndTime) {
				next = s.EndTime
			}

			// ratio of time spent in the current time range to overall duration of the snapshot
			ratio := next.Sub(p).Seconds() / totalDuration.Seconds()

			pbt[point] = valueHandler.Aggregate(pbt[point], value, ratio)
		}
	}

	// convert output to a map of time series with sorted points
	result := map[string]TimeSeries[TValue]{}

	for id, t := range ts {
		var timeSeries TimeSeries[TValue]

		for t, v := range t {
			timeSeries = append(timeSeries, TimeSeriesPoint[TValue]{
				Time:  t,
				Value: v,
			})
		}

		sort.Slice(timeSeries, func(i, j int) bool {
			return timeSeries[i].Time.Before(timeSeries[j].Time)
		})

		result[id] = timeSeries
	}

	return result
}
