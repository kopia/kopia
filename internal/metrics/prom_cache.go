package metrics

import (
	"maps"
	"slices"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	prometheusCounterSuffix = "_total"
	prometheusPrefix        = "kopia_"
)

//nolint:gochecknoglobals
var (
	promCacheMutex sync.Mutex
	// +checklocks:promCacheMutex
	promCounters = map[string]*prometheus.CounterVec{}
	// +checklocks:promCacheMutex
	promHistograms = map[string]*prometheus.HistogramVec{}
	promGauges     = map[string]*prometheus.GaugeVec{}
)

func getPrometheusCounter(opts prometheus.CounterOpts, labels map[string]string) prometheus.Counter {
	promCacheMutex.Lock()
	defer promCacheMutex.Unlock()

	prom := promCounters[opts.Name]
	if prom == nil {
		prom = promauto.NewCounterVec(opts, mapKeys(labels))

		promCounters[opts.Name] = prom
	}

	return prom.WithLabelValues(mapValues(labels)...)
}

func getPrometheusGauge(opts prometheus.GaugeOpts, labels map[string]string) *prometheus.GaugeVec {
	promCacheMutex.Lock()
	defer promCacheMutex.Unlock()

	prom := promGauges[opts.Name]
	if prom == nil {
		prom = promauto.NewGaugeVec(opts, maps.Keys(labels))

		promGauges[opts.Name] = prom
	}

	return prom.MustCurryWith(prometheus.Labels(labels))
}

func getPrometheusHistogram(opts prometheus.HistogramOpts, labels map[string]string) prometheus.Observer { //nolint:gocritic
	promCacheMutex.Lock()
	defer promCacheMutex.Unlock()

	prom := promHistograms[opts.Name]
	if prom == nil {
		prom = promauto.NewHistogramVec(opts, mapKeys(labels))

		promHistograms[opts.Name] = prom
	}

	return prom.WithLabelValues(mapValues(labels)...)
}

func mapKeys[Map ~map[K]V, K comparable, V any](m Map) []K {
	return slices.AppendSeq(make([]K, 0, len(m)), maps.Keys(m))
}

func mapValues[Map ~map[K]V, K comparable, V any](m Map) []V {
	return slices.AppendSeq(make([]V, 0, len(m)), maps.Values(m))
}
