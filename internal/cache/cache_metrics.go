package cache

import "github.com/kopia/kopia/internal/metrics"

type metricsStruct struct {
	metricHitCount                *metrics.Counter
	metricHitBytes                *metrics.Counter
	metricMissCount               *metrics.Counter
	metricMalformedCacheDataCount *metrics.Counter
	metricMissBytes               *metrics.Counter
	metricMissErrors              *metrics.Counter
	metricStoreErrors             *metrics.Counter
}

func initMetricsStruct(mr *metrics.Registry, cacheID string) metricsStruct {
	labels := map[string]string{
		"cache": cacheID,
	}

	return metricsStruct{
		metricHitCount: mr.CounterInt64(
			"cache_hit",
			"Number of time content was retrieved from the cache", labels),

		metricHitBytes: mr.CounterInt64(
			"cache_hit_bytes",
			"Number of bytes retrieved from the cache", labels),

		metricMissCount: mr.CounterInt64(
			"cache_miss",
			"Number of time content was not found in the cache and fetched from the storage", labels),

		metricMalformedCacheDataCount: mr.CounterInt64(
			"cache_malformed",
			"Number of times malformed content was read from the cache", labels),

		metricMissBytes: mr.CounterInt64(
			"cache_miss_bytes",
			"Number of bytes retrieved from the underlying storage", labels),

		metricMissErrors: mr.CounterInt64(
			"cache_miss_errors",
			"Number of time content could not be found in the underlying storage", labels),

		metricStoreErrors: mr.CounterInt64(
			"cache_store_errors",
			"Number of time content could not be saved in the cache", labels),
	}
}

func (s *metricsStruct) reportMissError() {
	s.metricMissErrors.Add(1)
}

func (s *metricsStruct) reportMissBytes(length int64) {
	s.metricMissCount.Add(1)
	s.metricMissBytes.Add(length)
}

func (s *metricsStruct) reportHitBytes(length int64) {
	s.metricHitCount.Add(1)
	s.metricHitBytes.Add(length)
}

func (s *metricsStruct) reportMalformedData() {
	s.metricMalformedCacheDataCount.Add(1)
}

func (s *metricsStruct) reportStoreError() {
	s.metricStoreErrors.Add(1)
}
