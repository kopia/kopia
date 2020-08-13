package content

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

// content cache metrics.
var (
	metricContentCacheHitCount = stats.Int64(
		"kopia/content/cache/hit_count",
		"Number of time content was retrieved from the cache",
		stats.UnitDimensionless,
	)

	metricContentCacheHitBytes = stats.Int64(
		"kopia/content/cache/hit_bytes",
		"Number of bytes retrieved from the cache",
		stats.UnitBytes,
	)

	metricContentCacheMissCount = stats.Int64(
		"kopia/content/cache/miss_count",
		"Number of time content was not found in the cache and fetched from the storage",
		stats.UnitDimensionless,
	)

	metricContentCacheMissBytes = stats.Int64(
		"kopia/content/cache/missed_bytes",
		"Number of bytes retrieved from the underlying storage",
		stats.UnitBytes,
	)

	metricContentCacheMissErrors = stats.Int64(
		"kopia/content/cache/miss_error_count",
		"Number of time content could not be found in the underlying storage",
		stats.UnitDimensionless,
	)

	metricContentCacheStoreErrors = stats.Int64(
		"kopia/content/cache/store_error_count",
		"Number of time content could not be saved in the cache",
		stats.UnitDimensionless,
	)
)

func init() {
	if err := view.Register(
		simpleAggregation(metricContentCacheHitCount, view.Count()),
		simpleAggregation(metricContentCacheHitBytes, view.Sum()),
		simpleAggregation(metricContentCacheMissCount, view.Count()),
		simpleAggregation(metricContentCacheMissBytes, view.Sum()),
		simpleAggregation(metricContentCacheMissErrors, view.Count()),
		simpleAggregation(metricContentCacheStoreErrors, view.Count()),
	); err != nil {
		panic("unable to register opencensus views: " + err.Error())
	}
}
