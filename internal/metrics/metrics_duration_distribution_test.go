package metrics_test

import (
	"testing"
	"time"

	prommodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
)

func TestDurationDistribution_Nil(t *testing.T) {
	var e *metrics.Registry
	dist := e.DurationDistribution("aaa", "bbb", metrics.IOLatencyThresholds, nil)
	require.Nil(t, dist)
	dist.Observe(time.Second)
	require.Equal(t, metrics.DistributionState[time.Duration]{}, dist.Snapshot())
}

func TestSizeDistribution_Nil(t *testing.T) {
	var e *metrics.Registry
	cnt := e.SizeDistribution("aaa", "bbb", metrics.ISOBytesThresholds, nil)
	require.Nil(t, cnt)
	cnt.Observe(333)
}

func TestDurationDistribution_NoLabels(t *testing.T) {
	e := metrics.NewRegistry()
	cnt := e.DurationDistribution("some_dur_dist", "some-help", metrics.IOLatencyThresholds, nil)
	cnt.Observe(time.Second)

	h1 := mustFindMetric(t, "kopia_some_dur_dist_ms", prommodel.MetricType_HISTOGRAM, nil)
	require.Equal(t, uint64(1), h1.GetHistogram().GetSampleCount())
	require.Equal(t, 1000.0, h1.GetHistogram().GetSampleSum())

	cnt.Observe(30 * time.Second)

	h1 = mustFindMetric(t, "kopia_some_dur_dist_ms", prommodel.MetricType_HISTOGRAM, nil)
	require.Equal(t, uint64(2), h1.GetHistogram().GetSampleCount())
	require.Equal(t, 31000.0, h1.GetHistogram().GetSampleSum())
}

func TestDurationDistribution_WithLabels(t *testing.T) {
	e := metrics.NewRegistry()
	cnt1 := e.DurationDistribution("some_dur_dist2", "some-help", metrics.IOLatencyThresholds, map[string]string{"key1": "label1"})
	cnt2 := e.DurationDistribution("some_dur_dist2", "some-help", metrics.IOLatencyThresholds, map[string]string{"key1": "label2"})

	snap0 := cnt1.Snapshot()

	require.Equal(t, int64(0), snap0.Count)
	require.Equal(t, 0*time.Second, snap0.Min)
	require.Equal(t, 0*time.Second, snap0.Max)
	require.Equal(t, 0*time.Second, snap0.Sum)
	require.Equal(t, 0*time.Second, snap0.Mean())

	cnt1.Observe(time.Second)
	cnt2.Observe(time.Hour)

	h1 := mustFindMetric(t, "kopia_some_dur_dist2_ms", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label1"})
	h2 := mustFindMetric(t, "kopia_some_dur_dist2_ms", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label2"})
	require.Equal(t, uint64(1), h1.GetHistogram().GetSampleCount())
	require.Equal(t, 1000.0, h1.GetHistogram().GetSampleSum())
	require.Equal(t, uint64(1), h2.GetHistogram().GetSampleCount())
	require.Equal(t, 3600000.0, h2.GetHistogram().GetSampleSum())

	cnt1.Observe(30 * time.Second)
	cnt2.Observe(50 * time.Second)

	h1 = mustFindMetric(t, "kopia_some_dur_dist2_ms", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label1"})
	h2 = mustFindMetric(t, "kopia_some_dur_dist2_ms", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label2"})
	require.Equal(t, uint64(2), h1.GetHistogram().GetSampleCount())
	require.Equal(t, 31000.0, h1.GetHistogram().GetSampleSum())
	require.Equal(t, uint64(2), h2.GetHistogram().GetSampleCount())
	require.Equal(t, 3650000.0, h2.GetHistogram().GetSampleSum())

	snap1 := cnt1.Snapshot()

	require.Equal(t, int64(2), snap1.Count)
	require.Equal(t, time.Second, snap1.Min)
	require.Equal(t, 30*time.Second, snap1.Max)
	require.Equal(t, 31*time.Second, snap1.Sum)
	require.Equal(t, 15500*time.Millisecond, snap1.Mean())

	snap2 := cnt2.Snapshot()

	require.Equal(t, int64(2), snap2.Count)
	require.Equal(t, 50*time.Second, snap2.Min)
	require.Equal(t, time.Hour, snap2.Max)
	require.Equal(t, time.Hour+50*time.Second, snap2.Sum)
	require.Equal(t, 30*time.Minute+25*time.Second, snap2.Mean())
}

func TestSizeDistribution_WithLabels(t *testing.T) {
	e := metrics.NewRegistry()
	cnt1 := e.SizeDistribution("some_size_dist", "some-help", metrics.ISOBytesThresholds, map[string]string{"key1": "label1"})
	cnt2 := e.SizeDistribution("some_size_dist", "some-help", metrics.ISOBytesThresholds, map[string]string{"key1": "label2"})

	snap0 := cnt1.Snapshot()

	require.Equal(t, int64(0), snap0.Count)
	require.Equal(t, int64(0), snap0.Min)
	require.Equal(t, int64(0), snap0.Max)
	require.Equal(t, int64(0), snap0.Sum)
	require.Equal(t, int64(0), snap0.Mean())

	cnt1.Observe(1000)
	cnt2.Observe(1e6)

	h1 := mustFindMetric(t, "kopia_some_size_dist", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label1"})
	h2 := mustFindMetric(t, "kopia_some_size_dist", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label2"})
	require.Equal(t, uint64(1), h1.GetHistogram().GetSampleCount())
	require.Equal(t, 1000.0, h1.GetHistogram().GetSampleSum())
	require.Equal(t, uint64(1), h2.GetHistogram().GetSampleCount())
	require.Equal(t, 1.0e6, h2.GetHistogram().GetSampleSum())

	cnt1.Observe(300)
	cnt2.Observe(50e6)

	h1 = mustFindMetric(t, "kopia_some_size_dist", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label1"})
	h2 = mustFindMetric(t, "kopia_some_size_dist", prommodel.MetricType_HISTOGRAM, map[string]string{"key1": "label2"})
	require.Equal(t, uint64(2), h1.GetHistogram().GetSampleCount())
	require.Equal(t, 1300.0, h1.GetHistogram().GetSampleSum())
	require.Equal(t, uint64(2), h2.GetHistogram().GetSampleCount())
	require.Equal(t, 51.0e6, h2.GetHistogram().GetSampleSum())

	snap1 := cnt1.Snapshot()

	require.Equal(t, int64(2), snap1.Count)
	require.Equal(t, int64(300), snap1.Min)
	require.Equal(t, int64(1000), snap1.Max)
	require.Equal(t, int64(1300), snap1.Sum)
	require.Equal(t, int64(650), snap1.Mean())

	snap2 := cnt2.Snapshot()

	require.Equal(t, int64(2), snap2.Count)
	require.Equal(t, int64(1000000), snap2.Min)
	require.Equal(t, int64(50000000), snap2.Max)
	require.Equal(t, int64(51000000), snap2.Sum)
	require.Equal(t, int64(25500000), snap2.Mean())
}
