package metrics_test

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/repo/logging"
)

func TestMetricEmitter_Nil(t *testing.T) {
	var m *metrics.Registry

	m.Log(context.Background())
	m.Close(context.Background())
}

func TestMetricEmitter_NotNil(t *testing.T) {
	var buf bytes.Buffer

	ctx := logging.WithLogger(context.Background(), logging.ToWriter(&buf))

	r := metrics.NewRegistry()
	r.CounterInt64("c1", "h1", nil).Add(33)
	r.Gauge("g1", "h1", nil).Set(44)
	r.Throughput("t1", "h1", nil).Observe(44, time.Second)
	r.DurationDistribution("d1", "h1", metrics.IOLatencyThresholds, nil).Observe(33 * time.Second)
	r.SizeDistribution("s1", "h1", metrics.ISOBytesThresholds, nil).Observe(333)
	require.NoError(t, r.Log(ctx))
	require.NoError(t, r.Close(ctx))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	sort.Strings(lines)

	require.Equal(t, []string{
		"COUNTER\t{\"name\":\"c1\",\"value\":33}",
		"COUNTER\t{\"name\":\"t1_bytes\",\"value\":44}",
		"COUNTER\t{\"name\":\"t1_duration_nanos\",\"value\":1000000000}",
		"DURATION-DISTRIBUTION\t{\"name\":\"d1\",\"counters\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0],\"cnt\":1,\"sum\":\"33s\",\"min\":\"33s\",\"avg\":\"33s\",\"max\":\"33s\"}",
		"GAUGE\t{\"name\":\"g1\",\"value\":44}",
		"SIZE-DISTRIBUTION\t{\"name\":\"s1\",\"counters\":[0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],\"cnt\":1,\"sum\":333,\"min\":333,\"avg\":333,\"max\":333}",
	}, lines)
}
