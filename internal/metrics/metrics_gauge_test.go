package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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
	gauge := e.GaugeInt64("test_gauge", "test-help", nil)

	// Set a value to ensure the gauge is created
	gauge.Set(42)

	// Verify the gauge exists in the registry
	require.NotNil(t, e.GaugeInt64("test_gauge", "test-help", nil))

	// Verify the gauge exist in proemetheus registry
	metrics, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	found := false
	for _, m := range metrics {
		if *m.Name == "kopia_test_gauge" {
			found = true
			break
		}
	}
	require.True(t, found)

	// Remove the gauge
	e.RemoveGauge(gauge)

	// Verify the gauge is removed from the registry
	require.False(t, e.HasGauge("test_gauge", nil))

	// Verify the gauge is removed from Prometheus registry
	metrics, err = prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, m := range metrics {
		if *m.Name == "kopia_test_gauge" {
			t.Errorf("gauge was not removed from Prometheus registry")
		}
	}
}
