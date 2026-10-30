package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Counter represents a monotonically increasing int64 counter value.
type Counter struct {
	state atomic.Int64

	prom prometheus.Counter
}

// Add adds a value to a counter.
func (c *Counter) Add(v int64) {
	if c == nil {
		return
	}

	c.prom.Add(float64(v))
	c.state.Add(v)
}

// newState initializes counter state and returns previous state or nil.
func (c *Counter) newState() int64 {
	return c.state.Swap(0)
}

// Snapshot captures the momentary state of a counter.
func (c *Counter) Snapshot(reset bool) int64 {
	if c == nil {
		return 0
	}

	if reset {
		return c.newState()
	}

	return c.state.Load()
}

// CounterInt64 gets a persistent int64 counter with the provided name.
func (r *Registry) CounterInt64(name, help string, labels map[string]string) *Counter {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	fullName := name + labelsSuffix(labels)

	c := r.allCounters[fullName]
	if c == nil {
		c = &Counter{
			prom: getPrometheusCounter(prometheus.CounterOpts{
				Name: prometheusPrefix + name + prometheusCounterSuffix,
				Help: help,
			}, labels),
		}

		c.newState()
		r.allCounters[fullName] = c
	}

	return c
}
