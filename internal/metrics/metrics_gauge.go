package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// Gauge represents a value that can go up and down.
type Gauge struct {
	state atomic.Int64

	prom *prometheus.GaugeVec
}

// Set sets the gauge to a specific value.
func (g *Gauge) Set(v int64) {
	if g == nil {
		return
	}

	g.prom.With(prometheus.Labels{}).Set(float64(v))

	g.state.Store(v)
}

// Add adds a value to the gauge.
func (g *Gauge) Add(v int64) {
	if g == nil {
		return
	}

	g.prom.With(prometheus.Labels{}).Add(float64(v))
	g.state.Add(v)
}

// RemoveGauge removes the gauge from the registry.
func (r *Registry) RemoveGauge(g *Gauge) {
	if r == nil || g == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for fullName, gauge := range r.allGauges {
		if gauge == g {
			delete(r.allGauges, fullName)
			g.prom.Delete(prometheus.Labels{})

			return
		}
	}
}

// RemoveAllGauges remove all gauges from the registry.
func (r *Registry) RemoveAllGauges() {
	for _, gauge := range r.allGauges {
		r.RemoveGauge(gauge)
	}
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

// HasGauge returns true if gauge exist in allGauges map.
func (r *Registry) HasGauge(name string, labels map[string]string) bool {
	fullName := name + labelsSuffix(labels)
	_, ok := r.allGauges[fullName]

	return ok
}
