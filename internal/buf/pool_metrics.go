package buf

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var tagKeyPool = tag.MustNewKey("pool")

// buffer pool metrics.
var (
	metricPoolAllocatedBuffers = stats.Int64(
		"kopia/bufferpool/allocated_buffers",
		"Number of buffers allocated from a pool",
		stats.UnitDimensionless,
	)

	metricPoolAllocatedBytes = stats.Int64(
		"kopia/bufferpool/allocated_bytes",
		"Number of bytes allocated from a pool",
		stats.UnitDimensionless,
	)

	metricPoolReleasedBuffers = stats.Int64(
		"kopia/bufferpool/released_buffers",
		"Number of buffers released back to the pool",
		stats.UnitBytes,
	)

	metricPoolReleasedBytes = stats.Int64(
		"kopia/bufferpool/released_bytes",
		"Number of bytes released from a pool",
		stats.UnitBytes,
	)

	metricPoolOutstandingBuffers = stats.Int64(
		"kopia/bufferpool/outstanding_buffers",
		"Number of buffers allocated from a pool but not returned yet",
		stats.UnitBytes,
	)

	metricPoolOutstandingBytes = stats.Int64(
		"kopia/bufferpool/outstanding_bytes",
		"Number of bytes allocated from a pool but not returned yet",
		stats.UnitBytes,
	)

	metricPoolNumSegments = stats.Int64(
		"kopia/bufferpool/num_segments",
		"Number of segments in the pool",
		stats.UnitDimensionless,
	)
)

func aggregateByPool(m stats.Measure, agg *view.Aggregation) *view.View {
	return &view.View{
		Name:        m.Name(),
		Aggregation: agg,
		Description: m.Description(),
		Measure:     m,
		TagKeys:     []tag.Key{tagKeyPool},
	}
}

func init() {
	if err := view.Register(
		aggregateByPool(metricPoolAllocatedBytes, view.LastValue()),
		aggregateByPool(metricPoolOutstandingBytes, view.LastValue()),
		aggregateByPool(metricPoolReleasedBytes, view.LastValue()),
		aggregateByPool(metricPoolAllocatedBuffers, view.LastValue()),
		aggregateByPool(metricPoolOutstandingBuffers, view.LastValue()),
		aggregateByPool(metricPoolReleasedBuffers, view.LastValue()),
		aggregateByPool(metricPoolNumSegments, view.LastValue()),
	); err != nil {
		panic("unable to register opencensus views: " + err.Error())
	}
}
