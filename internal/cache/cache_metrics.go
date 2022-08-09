package cache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint:gochecknoglobals,promlinter
var (
	metricHitCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_cache_hit_count",
		Help: "Number of time content was retrieved from the cache",
	})

	metricHitBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_cache_hit_bytes",
		Help: "Number of bytes retrieved from the cache",
	})

	metricMissCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_cache_miss_count",
		Help: "Number of time content was not found in the cache and fetched from the storage",
	})

	metricMalformedCacheDataCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_cache_malformed",
		Help: "Number of times malformed content was read from the cache",
	})

	metricMissBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_cache_missed_bytes",
		Help: "Number of bytes retrieved from the underlying storage",
	})

	metricMissErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_cache_miss_error_count",
		Help: "Number of time content could not be found in the underlying storage",
	})

	metricStoreErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_cache_store_error_count",
		Help: "Number of time content could not be saved in the cache",
	})
)

func reportMissError() {
	metricMissErrors.Inc()
}

func reportMissBytes(length int64) {
	metricMissCount.Inc()
	metricMissBytes.Add(float64(length))
}

func reportHitBytes(length int64) {
	metricHitCount.Inc()
	metricHitBytes.Add(float64(length))
}

func reportMalformedData() {
	metricMalformedCacheDataCount.Inc()
}

func reportStoreError() {
	metricStoreErrors.Inc()
}
