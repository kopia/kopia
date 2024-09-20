package metrics_test

import (
	"testing"

	prommodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
)

func TestGauge_Nil(t *testing.T) {
	var e *metrics.Registry
	gauge := e.GaugeInt64("aaa", "bbb", nil)
	require.Nil(t, gauge)
	gauge.Set(33)
	require.Equal(t, int64(0), gauge.Snapshot(false))
}

func TestGauge_NoLabels(t *testing.T) {
	e := metrics.NewRegistry()
	gauge := e.GaugeInt64("some_gauge", "some-help", nil)

	gauge.Set(0)

	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil).
			GetGauge().GetValue())
	gauge.Set(33)
	require.Equal(t, 33.0,
		mustFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil).
			GetGauge().GetValue())
	gauge.Add(10)
	require.Equal(t, 43.0,
		mustFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil).
			GetGauge().GetValue())

	require.Equal(t, int64(43), gauge.Snapshot(false))

	require.Equal(t, int64(43), gauge.Snapshot(true)) // reset
	require.Equal(t, int64(0), gauge.Snapshot(false))
}

func TestGauge_WithLabels(t *testing.T) {
	e := metrics.NewRegistry()
	gauge1 := e.GaugeInt64("some_gauge2", "some-help", map[string]string{"key1": "label1"})
	gauge2 := e.GaugeInt64("some_gauge2", "some-help", map[string]string{"key1": "label2"})

	gauge1.Set(0)
	gauge2.Set(0)

	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"}).
			GetGauge().GetValue())
	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}).
			GetGauge().GetValue())
	gauge1.Set(33)
	gauge2.Set(44)
	require.Equal(t, 44.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}).
			GetGauge().GetValue())
	require.Equal(t, 33.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"}).
			GetGauge().GetValue())
	gauge1.Add(10)
	gauge2.Add(-10)
	require.Equal(t, 43.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"}).
			GetGauge().GetValue())
	require.Equal(t, 34.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}).
			GetGauge().GetValue())
}

func TestGauge_WithMultipleLabels(t *testing.T) {
	e := metrics.NewRegistry()
	gauge1 := e.GaugeInt64("last_snapshot_start_time", "Timestamp of the last snapshot start time", map[string]string{"host": "host1", "username": "user1", "path": "path1"})
	gauge2 := e.GaugeInt64("last_snapshot_start_time", "Timestamp of the last snapshot start time", map[string]string{"host": "host2", "username": "user2", "path": "path2"})

	gauge1.Set(33)
	gauge2.Set(44)
	require.Equal(t, 33.0,
		mustFindMetric(t, "kopia_last_snapshot_start_time", prommodel.MetricType_GAUGE, map[string]string{"host": "host1", "username": "user1", "path": "path1"}).
			GetGauge().GetValue())
	require.Equal(t, 44.0,
		mustFindMetric(t, "kopia_last_snapshot_start_time", prommodel.MetricType_GAUGE, map[string]string{"host": "host2", "username": "user2", "path": "path2"}).
			GetGauge().GetValue())
}

func TestGauge_RemoveGauge(t *testing.T) {
	e := metrics.NewRegistry()
	gauge := e.GaugeInt64("some_gauge", "test-help", nil)

	// Set a value to ensure the gauge is created
	gauge.Set(42)

	// Verify the gauge exists in the registry
	require.True(t, e.HasGauge("some_gauge", nil))

	// Verify the gauge exist in proemetheus registry
	require.Equal(t, 42.0,
		mustFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil).
			GetGauge().GetValue())

	// Remove the gauge
	e.RemoveGauge(gauge)

	// Verify the gauge is removed from the registry
	require.False(t, e.HasGauge("some_gauge", nil))

	// Verify the gauge is removed from Prometheus registry
	mustNotFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil)
}

func TestGauge_RemoveOneOfTwoGaugesWithLabels(t *testing.T) {
	e := metrics.NewRegistry()
	gauge1 := e.GaugeInt64("some_gauge_2", "test-help", map[string]string{"key1": "label1"})
	gauge2 := e.GaugeInt64("some_gauge_2", "test-help", map[string]string{"key1": "label2"})

	// Set values to ensure the gauges are created
	gauge1.Set(42)
	gauge2.Set(43)

	// Verify the gauge exists in the registry
	require.True(t, e.HasGauge("some_gauge_2", map[string]string{"key1": "label1"}))
	require.True(t, e.HasGauge("some_gauge_2", map[string]string{"key1": "label2"}))

	// Verify both gauges exist in prometheus registry
	require.Equal(t, 42.0,
		mustFindMetric(t, "kopia_some_gauge_2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"}).
			GetGauge().GetValue())
	require.Equal(t, 43.0,
		mustFindMetric(t, "kopia_some_gauge_2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}).
			GetGauge().GetValue())

	// Remove gauge1
	e.RemoveGauge(gauge1)

	// Verify gauge1 is removed from the registry
	require.False(t, e.HasGauge("some_gauge_2", map[string]string{"key1": "label1"}))

	// Verify gauge1 is removed from Prometheus registry
	mustNotFindMetric(t, "kopia_some_gauge_2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"})

	// Verify gauge2 still exists in the registry
	require.True(t, e.HasGauge("some_gauge_2", map[string]string{"key1": "label2"}))

	// Verify gauge2 still exists in Prometheus registry
	require.NotNil(t, mustFindMetric(t, "kopia_some_gauge_2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}))
}
