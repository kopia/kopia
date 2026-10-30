package metrics_test

import (
	"testing"
	"time"

	prommodel "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
)

func TestThroughput_Nil(t *testing.T) {
	var r *metrics.Registry

	th := r.Throughput("some_throughput", "some-help", nil)
	th.Observe(1, time.Second)
}

func TestThroughput_NotNil(t *testing.T) {
	r := metrics.NewRegistry()

	th := r.Throughput("some_throughput2", "some-help", nil)

	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_throughput2_bytes_total", prommodel.MetricType_COUNTER, nil).
			GetCounter().GetValue())
	require.Equal(t, 0.0,
		mustFindMetric(t, "kopia_some_throughput2_duration_nanos_total", prommodel.MetricType_COUNTER, nil).
			GetCounter().GetValue())

	th.Observe(500, 500*time.Millisecond)
	th.Observe(1, time.Second)

	require.Equal(t, 501.0,
		mustFindMetric(t, "kopia_some_throughput2_bytes_total", prommodel.MetricType_COUNTER, nil).
			GetCounter().GetValue())
	require.Equal(t, 1.5e9,
		mustFindMetric(t, "kopia_some_throughput2_duration_nanos_total", prommodel.MetricType_COUNTER, nil).
			GetCounter().GetValue())
}
