package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Gauge represents a value that can go up and down.
type Gauge struct {
	state atomic.Int64

	prom prometheus.Gauge
}

// Set sets the gauge to a specific value.
func (g *Gauge) Set(v int64) {
	if g == nil {
		return
	}

	g.prom.Set(float64(v))
	g.state.Store(v)
}

// Add adds a value to the gauge.
func (g *Gauge) Add(v int64) {
	if g == nil {
		return
	}

	g.prom.Add(float64(v))
	g.state.Add(v)
}

// newState initializes gauge state and returns previous state or nil.
func (g *Gauge) newState() int64 {
	return g.state.Swap(0)
}

// Snapshot captures the momentary state of a gauge.
func (g *Gauge) Snapshot(reset bool) int64 {
	if g == nil {
		return 0
	}

	if reset {
		return g.newState()
	}

	return g.state.Load()
}

// GaugeInt64 gets a persistent int64 gauge with the provided name.
func (r *Registry) GaugeInt64(name, help string, labels map[string]string) *Gauge {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	fullName := name + labelsSuffix(labels)

	g := r.allGauges[fullName]
	if g == nil {
		g = &Gauge{
			prom: getPrometheusGauge(prometheus.GaugeOpts{
				Name: prometheusPrefix + name,
				Help: help,
			}, labels),
		}

		g.newState()
		r.allGauges[fullName] = g
	}

	return g
}
