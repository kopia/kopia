package metrics_test

import (
	"testing"

	prommodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
)

func TestCounter_Nil(t *testing.T) {
	var e *metrics.Registry
	cnt := e.CounterInt64("aaa", "bbb", nil)
	require.Nil(t, cnt)
	cnt.Add(33)
	require.Equal(t, int64(0), cnt.Snapshot())
}

func TestCounter_NoLabels(t *testing.T) {
	e := metrics.NewRegistry()
	cnt := e.CounterInt64("some_gauge", "some-help", nil)

	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge_total", prommodel.MetricType_COUNTER, nil).
			GetCounter().GetValue())
	cnt.Add(33)
	require.Equal(t, 33.0,
		mustFindMetric(t, "kopia_some_gauge_total", prommodel.MetricType_COUNTER, nil).
			GetCounter().GetValue())
	cnt.Add(100)
	require.Equal(t, 133.0,
		mustFindMetric(t, "kopia_some_gauge_total", prommodel.MetricType_COUNTER, nil).
			GetCounter().GetValue())

	require.Equal(t, int64(133), cnt.Snapshot())
}

func TestCounter_WithLabels(t *testing.T) {
	e := metrics.NewRegistry()
	cnt1 := e.CounterInt64("some_gauge2", "some-help", map[string]string{"key1": "label1"})
	cnt2 := e.CounterInt64("some_gauge2", "some-help", map[string]string{"key1": "label2"})

	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge2_total", prommodel.MetricType_COUNTER, map[string]string{"key1": "label1"}).
			GetCounter().GetValue())
	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_gauge2_total", prommodel.MetricType_COUNTER, map[string]string{"key1": "label2"}).
			GetCounter().GetValue())
	cnt1.Add(33)
	cnt2.Add(44)
	require.Equal(t, 44.0,
		mustFindMetric(t, "kopia_some_gauge2_total", prommodel.MetricType_COUNTER, map[string]string{"key1": "label2"}).
			GetCounter().GetValue())
	require.Equal(t, 33.0,
		mustFindMetric(t, "kopia_some_gauge2_total", prommodel.MetricType_COUNTER, map[string]string{"key1": "label1"}).
			GetCounter().GetValue())
	cnt1.Add(100)
	cnt2.Add(100)
	require.Equal(t, 133.0,
		mustFindMetric(t, "kopia_some_gauge2_total", prommodel.MetricType_COUNTER, map[string]string{"key1": "label1"}).
			GetCounter().GetValue())
	require.Equal(t, 144.0,
		mustFindMetric(t, "kopia_some_gauge2_total", prommodel.MetricType_COUNTER, map[string]string{"key1": "label2"}).
			GetCounter().GetValue())
}
