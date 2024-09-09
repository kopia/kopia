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
	gauge1 := e.GaugeInt64("some_gauge_2", "some-help", map[string]string{"key1": "label1"})
	gauge2 := e.GaugeInt64("some_gauge_2", "some-help", map[string]string{"key1": "label2"})

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
