package metrics_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
)

func TestAggregation(t *testing.T) {
	s1 := metrics.Snapshot{
		Counters: map[string]int64{
			"counter1": 123,
			"counter2": 555,
		},
		SizeDistributions: map[string]*metrics.DistributionState[int64]{
			"dist1": {
				Min:              100,
				Max:              200,
				Count:            2,
				Sum:              300,
				BucketCounters:   []int64{1, 1, 0, 0},
				BucketThresholds: []int64{150, 250, 300},
			},
		},
		DurationDistributions: map[string]*metrics.DistributionState[time.Duration]{
			"dur1": {
				Min:              100 * time.Second,
				Max:              200 * time.Second,
				Count:            2,
				Sum:              300 * time.Second,
				BucketCounters:   []int64{1, 1, 0, 0},
				BucketThresholds: []time.Duration{150 * time.Second, 250 * time.Second, 300 * time.Second},
			},
		},
	}

	s2 := metrics.Snapshot{
		Counters: map[string]int64{
			"counter1": 234,
			"counter3": 666,
		},
		SizeDistributions: map[string]*metrics.DistributionState[int64]{
			"dist1": {
				Min:              50,
				Max:              210,
				Count:            2,
				Sum:              260,
				BucketCounters:   []int64{1, 1, 0, 0},
				BucketThresholds: []int64{150, 250, 300},
			},
		},
		DurationDistributions: map[string]*metrics.DistributionState[time.Duration]{
			"dur1": {
				Min:              50 * time.Second,
				Max:              210 * time.Second,
				Count:            2,
				Sum:              260 * time.Second,
				BucketCounters:   []int64{1, 1, 0, 0},
				BucketThresholds: []time.Duration{150 * time.Second, 250 * time.Second, 300 * time.Second},
			},
		},
	}

	s3 := metrics.Snapshot{
		Counters: map[string]int64{
			"counter1": 3,
		},
	}

	s4 := metrics.Snapshot{
		Counters: map[string]int64{
			"counter4": 777,
		},
	}

	res := metrics.AggregateSnapshots([]metrics.Snapshot{s1, s2, s3, s4})

	// counters are summed
	require.Equal(t, map[string]int64{
		"counter1": 360,
		"counter2": 555,
		"counter3": 666,
		"counter4": 777,
	}, res.Counters)

	require.JSONEq(t,
		`{"dist1":{"min":50,"max":210,"sum":560,"count":4,"buckets":[2,2,0,0]}}`,
		toJSON(res.SizeDistributions))

	require.JSONEq(t,
		`{"dur1":{"min":50000000000,"max":210000000000,"sum":560000000000,"count":4,"buckets":[2,2,0,0]}}`,
		toJSON(res.DurationDistributions))
}

func toJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<invalid>"
	}

	return string(b)
}
