package cache

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

// cache metrics.
var (
	MetricHitCount = stats.Int64(
		"kopia/content/cache/hit_count",
		"Number of time content was retrieved from the cache",
		stats.UnitDimensionless,
	)

	MetricHitBytes = stats.Int64(
		"kopia/content/cache/hit_bytes",
		"Number of bytes retrieved from the cache",
		stats.UnitBytes,
	)

	MetricMissCount = stats.Int64(
		"kopia/content/cache/miss_count",
		"Number of time content was not found in the cache and fetched from the storage",
		stats.UnitDimensionless,
	)

	MetricMalformedCacheDataCount = stats.Int64(
		"kopia/content/cache/malformed",
		"Number of times malformed content was read from the cache",
		stats.UnitDimensionless,
	)

	MetricMissBytes = stats.Int64(
		"kopia/content/cache/missed_bytes",
		"Number of bytes retrieved from the underlying storage",
		stats.UnitBytes,
	)

	MetricMissErrors = stats.Int64(
		"kopia/content/cache/miss_error_count",
		"Number of time content could not be found in the underlying storage",
		stats.UnitDimensionless,
	)

	MetricStoreErrors = stats.Int64(
		"kopia/content/cache/store_error_count",
		"Number of time content could not be saved in the cache",
		stats.UnitDimensionless,
	)
)

func simpleAggregation(m stats.Measure, agg *view.Aggregation) *view.View {
	return &view.View{
		Name:        m.Name(),
		Aggregation: agg,
		Description: m.Description(),
		Measure:     m,
	}
}

func init() {
	if err := view.Register(
		simpleAggregation(MetricHitCount, view.Count()),
		simpleAggregation(MetricHitBytes, view.Sum()),
		simpleAggregation(MetricMissCount, view.Count()),
		simpleAggregation(MetricMissBytes, view.Sum()),
		simpleAggregation(MetricMissErrors, view.Count()),
		simpleAggregation(MetricStoreErrors, view.Count()),
	); err != nil {
		panic("unable to register opencensus views: " + err.Error())
	}
}
