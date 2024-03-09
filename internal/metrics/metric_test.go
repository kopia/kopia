package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func mustFindMetric(t *testing.T, wantName string, wantType io_prometheus_client.MetricType, wantLabels map[string]string) *io_prometheus_client.Metric {
	t.Helper()

	mf, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, f := range mf {
		if f.GetName() != wantName {
			continue
		}

		if f.GetType() != wantType {
			continue
		}

		for _, l := range f.GetMetric() {
			if len(l.GetLabel()) != len(wantLabels) {
				continue
			}

			found := true

			for _, lab := range l.GetLabel() {
				if wantLabels[lab.GetName()] != lab.GetValue() {
					found = false
				}
			}

			if found {
				return l
			}
		}
	}

	for _, f := range mf {
		for _, l := range f.GetMetric() {
			t.Logf("  %v %v %v", f.GetName(), f.GetType(), l.GetLabel())
		}
	}

	require.Failf(t, "metric %v not found", wantName)

	return nil
}
