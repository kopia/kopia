package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Gauge is a metric representing momentary state.
type Gauge struct {
	state atomic.Int64

	prom prometheus.Gauge
}

// Set sets the value of a gauge.
func (c *Gauge) Set(v int64) {
	if c == nil {
		return
	}

	if c.prom != nil {
		c.prom.Set(float64(v))
	}

	c.state.Store(v)
}

// newState initializes counter state and returns previous state or 0.
func (c *Gauge) newState() int64 {
	return c.state.Swap(0)
}

// Snapshot captures a momentary state of the gauge.
func (c *Gauge) Snapshot() int64 {
	if c == nil {
		return 0
	}

	return c.state.Load()
}

// Gauge gets a persistent gauge with the provided name.
func (r *Registry) Gauge(name, help string, labels map[string]string) *Gauge {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	fullName := name + labelsSuffix(labels)

	c := r.allGauges[fullName]
	if c == nil {
		c = &Gauge{
			prom: getPrometheusGauge(prometheus.GaugeOpts{
				Name: prometheusPrefix + name,
				Help: help,
			}, labels),
		}
		c.newState()

		r.allGauges[fullName] = c
	}

	return c
}
