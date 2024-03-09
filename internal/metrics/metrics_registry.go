// Package metrics provides unified way of emitting metrics inside Kopia.
package metrics

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/releasable"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("metrics")

// Registry groups together all metrics stored in the repository and provides ways of accessing them.
type Registry struct {
	mu sync.Mutex

	// +checklocks:mu
	startTime time.Time

	allCounters              map[string]*Counter
	allThroughput            map[string]*Throughput
	allDurationDistributions map[string]*Distribution[time.Duration]
	allSizeDistributions     map[string]*Distribution[int64]
}

// Snapshot captures the state of all metrics.
type Snapshot struct {
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
	User      string    `json:"user"`
	Hostname  string    `json:"hostname"`

	Counters              map[string]int64                             `json:"counters"`
	DurationDistributions map[string]*DistributionState[time.Duration] `json:"durationDistributions"`
	SizeDistributions     map[string]*DistributionState[int64]         `json:"sizeDistributions"`
}

func (s *Snapshot) mergeFrom(other Snapshot) {
	for k, v := range other.Counters {
		s.Counters[k] += v
	}

	for k, v := range other.DurationDistributions {
		target := s.DurationDistributions[k]
		if target == nil {
			target = &DistributionState[time.Duration]{}
			s.DurationDistributions[k] = target
		}

		target.mergeFrom(v)
	}

	for k, v := range other.SizeDistributions {
		target := s.SizeDistributions[k]
		if target == nil {
			target = &DistributionState[int64]{}
			s.SizeDistributions[k] = target
		}

		target.mergeFrom(v)
	}
}

func createSnapshot() Snapshot {
	return Snapshot{
		Counters:              map[string]int64{},
		DurationDistributions: map[string]*DistributionState[time.Duration]{},
		SizeDistributions:     map[string]*DistributionState[int64]{},
	}
}

// Snapshot captures the snapshot of all metrics.
func (r *Registry) Snapshot(reset bool) Snapshot {
	s := createSnapshot()

	for k, c := range r.allCounters {
		s.Counters[k] = c.Snapshot(reset)
	}

	for k, c := range r.allDurationDistributions {
		s.DurationDistributions[k] = c.Snapshot(reset)
	}

	for k, c := range r.allSizeDistributions {
		s.SizeDistributions[k] = c.Snapshot(reset)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	s.StartTime = r.startTime
	s.EndTime = clock.Now()

	if reset {
		r.startTime = clock.Now()
	}

	return s
}

// Close closes the metrics registry.
func (r *Registry) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}

	releasable.Released("metric-registry", r)

	return nil
}

// Log logs all metrics in the registry.
func (r *Registry) Log(ctx context.Context) {
	if r == nil {
		return
	}

	s := r.Snapshot(false)

	for n, val := range s.Counters {
		log(ctx).Debugw("COUNTER", "name", n, "value", val)
	}

	for n, st := range s.DurationDistributions {
		log(ctx).Debugw("DURATION-DISTRIBUTION", "name", n, "counters", st.BucketCounters, "cnt", st.Count, "sum", st.Sum, "min", st.Min, "avg", st.Mean(), "max", st.Max)
	}

	for n, st := range s.SizeDistributions {
		if st.Count > 0 {
			log(ctx).Debugw("SIZE-DISTRIBUTION", "name", n, "counters", st.BucketCounters, "cnt", st.Count, "sum", st.Sum, "min", st.Min, "avg", st.Mean(), "max", st.Max)
		}
	}
}

// NewRegistry returns new metrics registry.
func NewRegistry() *Registry {
	r := &Registry{
		startTime: clock.Now(),

		allCounters:              map[string]*Counter{},
		allDurationDistributions: map[string]*Distribution[time.Duration]{},
		allSizeDistributions:     map[string]*Distribution[int64]{},
		allThroughput:            map[string]*Throughput{},
	}

	releasable.Created("metric-registry", r)

	return r
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
