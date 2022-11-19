package metrics_test

import (
	"testing"

	prommodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
)

func TestGauge_Nil(t *testing.T) {
	var mr *metrics.Registry
	g := mr.Gauge("aaa", "bbb", nil)
	require.Nil(t, g)
	g.Set(33)
	require.Equal(t, int64(0), g.Snapshot(false))
}

func TestGauge_NoLabels(t *testing.T) {
	mr := metrics.NewRegistry()
	g := mr.Gauge("some_gauge", "some-help", nil)

	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil).
			GetGauge().GetValue())
	g.Set(33)
	require.Equal(t, 33.0,
		mustFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil).
			GetGauge().GetValue())
	g.Set(133)
	require.Equal(t, 133.0,
		mustFindMetric(t, "kopia_some_gauge", prommodel.MetricType_GAUGE, nil).
			GetGauge().GetValue())

	require.Equal(t, int64(133), g.Snapshot(false))
	require.Equal(t, int64(133), g.Snapshot(true)) // reset
	require.Equal(t, int64(0), g.Snapshot(false))
}

func TestGauge_WithLabels(t *testing.T) {
	mr := metrics.NewRegistry()
	g1 := mr.Gauge("some_gauge2", "some-help", map[string]string{"key1": "label1"})
	g2 := mr.Gauge("some_gauge2", "some-help", map[string]string{"key1": "label2"})

	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"}).
			GetGauge().GetValue())
	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}).
			GetGauge().GetValue())
	g1.Set(33)
	g2.Set(44)
	require.Equal(t, 44.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}).
			GetGauge().GetValue())
	require.Equal(t, 33.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"}).
			GetGauge().GetValue())
	g1.Set(133)
	g2.Set(144)
	require.Equal(t, 133.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label1"}).
			GetGauge().GetValue())
	require.Equal(t, 144.0,
		mustFindMetric(t, "kopia_some_gauge2", prommodel.MetricType_GAUGE, map[string]string{"key1": "label2"}).
			GetGauge().GetValue())
}
