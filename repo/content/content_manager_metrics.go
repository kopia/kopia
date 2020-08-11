package content

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

// content cache metrics.
var (
	metricContentGetCount = stats.Int64(
		"kopia/content/get_count",
		"Number of time GetContent() was called",
		stats.UnitDimensionless,
	)

	metricContentGetNotFoundCount = stats.Int64(
		"kopia/content/get_not_found_count",
		"Number of time GetContent() was called and the result was not found",
		stats.UnitDimensionless,
	)

	metricContentGetErrorCount = stats.Int64(
		"kopia/content/get_error_count",
		"Number of time GetContent() was called and the result was an error",
		stats.UnitDimensionless,
	)

	metricContentGetBytes = stats.Int64(
		"kopia/content/get_bytes",
		"Number of bytes retrieved using GetContent",
		stats.UnitBytes,
	)

	metricContentWriteContentCount = stats.Int64(
		"kopia/content/write_count",
		"Number of time WriteContent() was called",
		stats.UnitDimensionless,
	)

	metricContentWriteContentBytes = stats.Int64(
		"kopia/content/write_bytes",
		"Number of bytes passed to WriteContent()",
		stats.UnitBytes,
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
		simpleAggregation(metricContentGetCount, view.Count()),
		simpleAggregation(metricContentGetNotFoundCount, view.Count()),
		simpleAggregation(metricContentGetErrorCount, view.Count()),
		simpleAggregation(metricContentGetBytes, view.Sum()),
		simpleAggregation(metricContentWriteContentCount, view.Count()),
	); err != nil {
		panic("unable to register opencensus views: " + err.Error())
	}
}
