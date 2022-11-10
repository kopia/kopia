package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/exp/maps"
)

const (
	prometheusCounterSuffix = "_total"
	prometheusPrefix        = "kopia_"
)

//nolint:gochecknoglobals
var (
	promCacheMutex sync.Mutex
	promCounters   = map[string]*prometheus.CounterVec{}
	promGauges     = map[string]*prometheus.GaugeVec{}
	promHistograms = map[string]*prometheus.HistogramVec{}
)

func getPrometheusCounter(opts prometheus.CounterOpts, labels map[string]string) prometheus.Counter {
	promCacheMutex.Lock()
	defer promCacheMutex.Unlock()

	prom := promCounters[opts.Name]
	if prom == nil {
		prom = promauto.NewCounterVec(opts, maps.Keys(labels))

		promCounters[opts.Name] = prom
	}

	return prom.WithLabelValues(maps.Values(labels)...)
}

func getPrometheusGauge(opts prometheus.GaugeOpts, labels map[string]string) prometheus.Gauge {
	promCacheMutex.Lock()
	defer promCacheMutex.Unlock()

	prom := promGauges[opts.Name]
	if prom == nil {
		prom = promauto.NewGaugeVec(opts, maps.Keys(labels))

		promGauges[opts.Name] = prom
	}

	return prom.WithLabelValues(maps.Values(labels)...)
}

func getPrometheusHistogram(opts prometheus.HistogramOpts, labels map[string]string) prometheus.Observer {
	promCacheMutex.Lock()
	defer promCacheMutex.Unlock()

	prom := promHistograms[opts.Name]
	if prom == nil {
		prom = promauto.NewHistogramVec(opts, maps.Keys(labels))

		promHistograms[opts.Name] = prom
	}

	return prom.WithLabelValues(maps.Values(labels)...)
}
