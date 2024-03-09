package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/constraints"
)

// DistributionState captures momentary state of a Distribution.
type DistributionState[T constraints.Float | constraints.Integer] struct {
	Min              T       `json:"min"`
	Max              T       `json:"max"`
	Sum              T       `json:"sum"`
	Count            int64   `json:"count"`
	BucketCounters   []int64 `json:"buckets"`
	BucketThresholds []T     `json:"-"`
}

func (s *DistributionState[T]) mergeFrom(other *DistributionState[T]) {
	s.mergeScaledFrom(other, 1)
}

func (s *DistributionState[T]) mergeScaledFrom(other *DistributionState[T], scale float64) {
	if s.Count == 0 {
		s.Min = other.Min
		s.Max = other.Max
	}

	if other.Max > s.Max {
		s.Max = other.Max
	}

	if other.Min < s.Min {
		s.Min = other.Min
	}

	s.Sum += other.Sum
	s.Count += other.Count

	if s.BucketCounters == nil {
		s.BucketCounters = make([]int64, len(other.BucketCounters))
	}

	if s.BucketThresholds == nil {
		s.BucketThresholds = other.BucketThresholds
	}

	if len(s.BucketCounters) == len(other.BucketCounters) {
		for i, v := range other.BucketCounters {
			s.BucketCounters[i] += int64(float64(v) * scale)
		}
	}
}

// Mean returns arithmetic mean value captured in the distribution.
func (s *DistributionState[T]) Mean() T {
	if s.Count == 0 {
		return 0
	}

	return s.Sum / T(s.Count)
}

// Distribution measures distribution/summary of values.
type Distribution[T constraints.Integer | constraints.Float] struct {
	mu               sync.Mutex
	state            atomic.Pointer[DistributionState[T]] // +checklocksignore
	bucketThresholds []T                                  // +checklocksignore

	prom            prometheus.Observer
	prometheusScale float64
}

// Observe adds the provided observation value to the summary.
func (d *Distribution[T]) Observe(value T) {
	if d == nil {
		return
	}

	st := d.state.Load()
	b := bucketForThresholds(d.bucketThresholds, value)

	d.prom.Observe(float64(value) / d.prometheusScale)

	d.mu.Lock()
	defer d.mu.Unlock()

	st.Sum += value
	st.Count++

	if st.Count == 1 {
		st.Max = value
		st.Min = value
	} else {
		if value > st.Max {
			st.Max = value
		}

		if value < st.Min {
			st.Min = value
		}
	}

	st.BucketCounters[b]++
}

// Snapshot returns a snapshot of the distribution state.
func (d *Distribution[T]) Snapshot(reset bool) *DistributionState[T] {
	if d == nil {
		return &DistributionState[T]{}
	}

	if reset {
		return d.newState()
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	s := d.state.Load()

	return &DistributionState[T]{
		Sum:            s.Sum,
		Count:          s.Count,
		Min:            s.Min,
		Max:            s.Max,
		BucketCounters: append([]int64(nil), s.BucketCounters...),
	}
}

func (d *Distribution[T]) newState() *DistributionState[T] {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.state.Swap(&DistributionState[T]{
		BucketCounters:   make([]int64, len(d.bucketThresholds)+1),
		BucketThresholds: d.bucketThresholds,
	})
}

// DurationDistribution gets a persistent duration distribution with the provided name.
func (r *Registry) DurationDistribution(name, help string, thresholds *Thresholds[time.Duration], labels map[string]string) *Distribution[time.Duration] {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	fullName := name + labelsSuffix(labels)
	h := r.allDurationDistributions[fullName]

	if h == nil {
		var promBuckets []float64

		// convert from nanoseconds to seconds
		promScale := thresholds.promScale
		for _, v := range thresholds.values {
			promBuckets = append(promBuckets, float64(v)/promScale)
		}

		h = &Distribution[time.Duration]{
			bucketThresholds: thresholds.values,
			prometheusScale:  promScale,
			prom: getPrometheusHistogram(
				prometheus.HistogramOpts{
					Name:    prometheusPrefix + name + thresholds.prometheusSuffix,
					Help:    help,
					Buckets: promBuckets,
				},
				labels,
			),
		}

		h.newState()

		r.allDurationDistributions[fullName] = h
	}

	return h
}

// SizeDistribution gets a persistent size distribution with the provided name.
func (r *Registry) SizeDistribution(name, help string, thresholds *Thresholds[int64], labels map[string]string) *Distribution[int64] {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	fullName := name + labelsSuffix(labels)
	h := r.allSizeDistributions[fullName]

	if h == nil {
		var promBuckets []float64

		// convert from nanoseconds to seconds
		promScale := thresholds.promScale
		for _, v := range thresholds.values {
			promBuckets = append(promBuckets, float64(v)/promScale)
		}

		h = &Distribution[int64]{
			bucketThresholds: thresholds.values,
			prometheusScale:  promScale,
			prom: getPrometheusHistogram(
				prometheus.HistogramOpts{
					Name:    prometheusPrefix + name + thresholds.prometheusSuffix,
					Help:    help,
					Buckets: promBuckets,
				},
				labels,
			),
		}

		h.newState()

		r.allSizeDistributions[fullName] = h
	}

	return h
}
