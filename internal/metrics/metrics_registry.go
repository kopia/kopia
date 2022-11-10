// Package metrics provides unified way of emitting metrics inside Kopia.
package metrics

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("metrics")

// Registry groups together all metrics stored in the repository and provides ways of accessing them.
type Registry struct {
	mu                       sync.Mutex
	allCounters              map[string]*Counter
	allGauges                map[string]*Gauge
	allThroughput            map[string]*Throughput
	allDurationDistributions map[string]*Distribution[time.Duration]
	allSizeDistributions     map[string]*Distribution[int64]
}

// Snapshot captures the state of all metrics.
type Snapshot struct {
	Counters              map[string]int64
	Gauges                map[string]int64
	DurationDistributions map[string]DistributionState[time.Duration]
	SizeDistributions     map[string]DistributionState[int64]
}

// Snapshot captures the snapshot of all metrics.
func (r *Registry) Snapshot() Snapshot {
	s := Snapshot{
		Counters:              map[string]int64{},
		Gauges:                map[string]int64{},
		DurationDistributions: map[string]DistributionState[time.Duration]{},
		SizeDistributions:     map[string]DistributionState[int64]{},
	}

	for k, c := range r.allCounters {
		s.Counters[k] = c.Snapshot()
	}

	for k, c := range r.allGauges {
		s.Gauges[k] = c.Snapshot()
	}

	for k, c := range r.allDurationDistributions {
		s.DurationDistributions[k] = c.Snapshot()
	}

	for k, c := range r.allSizeDistributions {
		s.SizeDistributions[k] = c.Snapshot()
	}

	return s
}

// Close closes the metrics registry.
func (r *Registry) Close(ctx context.Context) error {
	return nil
}

// Log logs all metrics in the registry.
func (r *Registry) Log(ctx context.Context) error {
	if r == nil {
		return nil
	}

	s := r.Snapshot()

	for n, val := range s.Counters {
		log(ctx).Debugw("COUNTER", "name", n, "value", val)
	}

	for n, val := range s.Gauges {
		log(ctx).Debugw("GAUGE", "name", n, "value", val)
	}

	for n, st := range s.DurationDistributions {
		log(ctx).Debugw("DURATION-DISTRIBUTION", "name", n, "counters", st.BucketCounters, "cnt", st.Count, "sum", st.Sum, "min", st.Min, "avg", st.Mean(), "max", st.Max)
	}

	for n, st := range s.SizeDistributions {
		if st.Count > 0 {
			log(ctx).Debugw("SIZE-DISTRIBUTION", "name", n, "counters", st.BucketCounters, "cnt", st.Count, "sum", st.Sum, "min", st.Min, "avg", st.Mean(), "max", st.Max)
		}
	}

	return nil
}

// NewRegistry returns new metrics registry.
func NewRegistry() *Registry {
	e := &Registry{
		allCounters:              map[string]*Counter{},
		allGauges:                map[string]*Gauge{},
		allDurationDistributions: map[string]*Distribution[time.Duration]{},
		allSizeDistributions:     map[string]*Distribution[int64]{},
		allThroughput:            map[string]*Throughput{},
	}

	return e
}

func labelsSuffix(l map[string]string) string {
	if len(l) == 0 {
		return ""
	}

	var params []string
	for k, v := range l {
		params = append(params, k+":"+v)
	}

	return "[" + strings.Join(params, ";") + "]"
}
