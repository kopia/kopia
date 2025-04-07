package metrics_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestCounterTimeSeries(t *testing.T) {
	dayStart := dayOf(2021, 1, 1)
	hour12 := dayStart.Add(12 * time.Hour)
	hour13 := dayStart.Add(13 * time.Hour)
	hour14 := dayStart.Add(14 * time.Hour)
	hour15 := dayStart.Add(15 * time.Hour)

	const counterName = "counter1"

	user1host1Snapshot := func(startTime, endTime time.Time, val int64) *metrics.Snapshot {
		return &metrics.Snapshot{
			StartTime: startTime,
			EndTime:   endTime,
			User:      "user1",
			Hostname:  "host1",
			Counters: map[string]int64{
				counterName: val,
			},
		}
	}

	user2host1Snapshot := func(startTime, endTime time.Time, val int64) *metrics.Snapshot {
		return &metrics.Snapshot{
			StartTime: startTime,
			EndTime:   endTime,
			User:      "user2",
			Hostname:  "host1",
			Counters: map[string]int64{
				counterName: val,
			},
		}
	}

	user3host2Snapshot := func(startTime, endTime time.Time, val int64) *metrics.Snapshot {
		return &metrics.Snapshot{
			StartTime: startTime,
			EndTime:   endTime,
			User:      "user3",
			Hostname:  "host2",
			Counters: map[string]int64{
				counterName: val,
			},
		}
	}

	cases := []struct {
		name           string
		snapshots      []*metrics.Snapshot
		want           map[string]metrics.TimeSeries[int64]
		aggregateBy    metrics.AggregateByFunc
		timeResolution metrics.TimeResolutionFunc
	}{
		{
			name: "single counter value within one time period",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(hour12.Add(5*time.Minute), hour12.Add(55*time.Minute), 100),
			},
			want: map[string]metrics.TimeSeries[int64]{
				"user1@host1": {{hour12, 100}},
			},
			timeResolution: metrics.TimeResolutionByHour,
		},
		{
			name: "3 independent counter value within one time period (12:05..12:55)",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(hour12.Add(5*time.Minute), hour12.Add(55*time.Minute), 111),
				user2host1Snapshot(hour12.Add(5*time.Minute), hour12.Add(55*time.Minute), 222),
				user3host2Snapshot(hour12.Add(5*time.Minute), hour12.Add(55*time.Minute), 333),
			},
			want: map[string]metrics.TimeSeries[int64]{
				"user1@host1": {{hour12, 111}},
				"user2@host1": {{hour12, 222}},
				"user3@host2": {{hour12, 333}},
			},
			timeResolution: metrics.TimeResolutionByHour,
		},
		{
			name: "3 independent counter value within different time period",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(hour12, hour13, 111),
				user2host1Snapshot(hour13, hour14, 222),
				user3host2Snapshot(hour14, hour15, 333),
			},
			want: map[string]metrics.TimeSeries[int64]{
				"user1@host1": {{hour12, 111}},
				"user2@host1": {{hour13, 222}},
				"user3@host2": {{hour14, 333}},
			},
			timeResolution: metrics.TimeResolutionByHour,
		},
		{
			name: "3 independent counter value within different time period, aggregated by host",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(hour12, hour13, 111),
				user2host1Snapshot(hour13, hour14, 222),
				user3host2Snapshot(hour14, hour15, 333),
			},
			want: map[string]metrics.TimeSeries[int64]{
				"host1": {{hour12, 111}, {hour13, 222}},
				"host2": {{hour14, 333}},
			},
			timeResolution: metrics.TimeResolutionByHour,
			aggregateBy:    metrics.AggregateByHost,
		},
		{
			name: "3 independent counter value within different time period, aggregated together",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(hour12, hour13, 111),
				user1host1Snapshot(hour13, hour14, 11),
				user2host1Snapshot(hour13, hour14, 222),
				user1host1Snapshot(hour14, hour15, 22),
				user3host2Snapshot(hour14, hour15, 333),
			},
			want: map[string]metrics.TimeSeries[int64]{
				"*": {{hour12, 111}, {hour13, 233}, {hour14, 355}},
			},
			timeResolution: metrics.TimeResolutionByHour,
			aggregateBy:    metrics.AggregateAll,
		},
		{
			name: "single counter spanning 3 time periods",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(hour12.Add(45*time.Minute), hour14.Add(45*time.Minute), 200),
			},
			want: map[string]metrics.TimeSeries[int64]{
				// 200 will be proportionally attributed to 3 hours it spans
				"user1@host1": {{hour12, 25}, {hour13, 100}, {hour14, 75}},
			},
			timeResolution: metrics.TimeResolutionByHour,
		},
		{
			name: "single counter spanning 4 time periods",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(hour12.Add(30*time.Minute), hour15.Add(30*time.Minute), 300),
			},
			want: map[string]metrics.TimeSeries[int64]{
				// 200 will be proportionally attributed to 4 hours it spans
				"user1@host1": {{hour12, 50}, {hour13, 100}, {hour14, 100}, {hour15, 50}},
			},
			timeResolution: metrics.TimeResolutionByHour,
		},
		{
			name: "time resolution by month",
			snapshots: []*metrics.Snapshot{
				// 3-month-long aggregation
				user1host1Snapshot(
					monthOf(2021, 1), monthOf(2021, 4), 300),
			},
			want: map[string]metrics.TimeSeries[int64]{
				// 300 will be proportionally attributed to 3 months it spans
				// notice, because February is 28 days long and others are 31, the proportion is not exactly 100
				"user1@host1": {{monthOf(2021, 1), 103}, {monthOf(2021, 2), 93}, {monthOf(2021, 3), 103}},
			},
			timeResolution: metrics.TimeResolutionByMonth,
		},
		{
			name: "default time resolution by day",
			snapshots: []*metrics.Snapshot{
				user1host1Snapshot(dayOf(2021, 1, 1), dayOf(2021, 1, 11), 1000),
			},
			want: map[string]metrics.TimeSeries[int64]{
				"user1@host1": {
					{dayOf(2021, 1, 1), 100},
					{dayOf(2021, 1, 2), 100},
					{dayOf(2021, 1, 3), 100},
					{dayOf(2021, 1, 4), 100},
					{dayOf(2021, 1, 5), 100},
					{dayOf(2021, 1, 6), 100},
					{dayOf(2021, 1, 7), 100},
					{dayOf(2021, 1, 8), 100},
					{dayOf(2021, 1, 9), 100},
					{dayOf(2021, 1, 10), 100},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testlogging.Context(t)

			ts := metrics.CreateTimeSeries(ctx, tc.snapshots, metrics.CounterValue(counterName), metrics.AggregateMetricsOptions{
				AggregateBy:    tc.aggregateBy,
				TimeResolution: tc.timeResolution,
			})

			require.Equal(t, tc.want, ts)
		})
	}
}

func TestAggregateDurationDistributions(t *testing.T) {
	const distName = "dist1"

	user1host1Snapshot := func(startTime, endTime time.Time, counters []int64) *metrics.Snapshot {
		return &metrics.Snapshot{
			StartTime: startTime,
			EndTime:   endTime,
			User:      "user1",
			Hostname:  "host1",
			DurationDistributions: map[string]*metrics.DistributionState[time.Duration]{
				distName: {BucketCounters: counters},
			},
		}
	}

	snapshots := []*metrics.Snapshot{
		user1host1Snapshot(dayOf(2021, 1, 1), dayOf(2021, 1, 11), []int64{50, 100, 150}),
		user1host1Snapshot(dayOf(2021, 1, 1), dayOf(2021, 1, 11), []int64{50, 100, 150}),
	}

	ctx := testlogging.Context(t)
	ts := metrics.CreateTimeSeries(ctx, snapshots, metrics.DurationDistributionValue(distName), metrics.AggregateMetricsOptions{
		TimeResolution: metrics.TimeResolutionByDay,
	})

	require.Len(t, ts, 1)

	ts0 := ts["user1@host1"]
	require.Len(t, ts0, 10)

	for _, p := range ts0 {
		// all distribution buckets are aggregated and scaled
		require.Equal(t, []int64{10, 20, 30}, p.Value.BucketCounters)
	}

	// no timeseries are returned for non-existing metric
	require.Empty(t, metrics.CreateTimeSeries(ctx, snapshots, metrics.DurationDistributionValue("no-such-metric"), metrics.AggregateMetricsOptions{
		TimeResolution: metrics.TimeResolutionByDay,
	}))
}

func TestAggregateSizeDistributions(t *testing.T) {
	const distName = "dist1"

	user1host1Snapshot := func(startTime, endTime time.Time, counters []int64) *metrics.Snapshot {
		return &metrics.Snapshot{
			StartTime: startTime,
			EndTime:   endTime,
			User:      "user1",
			Hostname:  "host1",
			SizeDistributions: map[string]*metrics.DistributionState[int64]{
				distName: {BucketCounters: counters},
			},
		}
	}

	snapshots := []*metrics.Snapshot{
		user1host1Snapshot(dayOf(2021, 1, 1), dayOf(2021, 1, 11), []int64{50, 100, 150}),
		user1host1Snapshot(dayOf(2021, 1, 1), dayOf(2021, 1, 11), []int64{50, 100, 150}),
	}

	ctx := testlogging.Context(t)
	ts := metrics.CreateTimeSeries(ctx, snapshots, metrics.SizeDistributionValue(distName), metrics.AggregateMetricsOptions{
		TimeResolution: metrics.TimeResolutionByDay,
	})

	require.Len(t, ts, 1)

	ts0 := ts["user1@host1"]
	require.Len(t, ts0, 10)

	for _, p := range ts0 {
		// all distribution buckets are aggregated and scaled
		require.Equal(t, []int64{10, 20, 30}, p.Value.BucketCounters)
	}

	// no timeseries are returned for non-existing metric
	require.Empty(t, metrics.CreateTimeSeries(ctx, snapshots, metrics.SizeDistributionValue("no-such-metric"), metrics.AggregateMetricsOptions{
		TimeResolution: metrics.TimeResolutionByDay,
	}))
}

func dayOf(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func monthOf(y int, m time.Month) time.Time {
	return dayOf(y, m, 1)
}
