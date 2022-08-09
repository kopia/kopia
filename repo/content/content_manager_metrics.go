package content

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// content cache metrics.
//
//nolint:gochecknoglobals,promlinter
var (
	metricContentGetCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_get_count",
		Help: "Number of time GetContent() was called",
	})
	metricContentGetNotFoundCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_get_not_found_count",
		Help: "Number of time GetContent() was called and the result was not found",
	})
	metricContentGetErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_get_error_count",
		Help: "Number of time GetContent() was called and the result was an error",
	})
	metricContentGetBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_get_bytes",
		Help: "Number of bytes retrieved using GetContent",
	})
	metricContentWriteContentCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_write_count",
		Help: "Number of time WriteContent() was called",
	})
	metricContentWriteContentBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kopia_content_write_bytes",
		Help: "Number of bytes passed to WriteContent()",
	})
)

func reportContentWriteBytes(length int64) {
	metricContentWriteContentCount.Inc()
	metricContentWriteContentBytes.Add(float64(length))
}

func reportContentGetBytes(length int64) {
	metricContentGetCount.Inc()
	metricContentGetBytes.Add(float64(length))
}

func reportContentGetError() {
	metricContentGetErrorCount.Inc()
}

func reportContentGetNotFound() {
	metricContentGetNotFoundCount.Inc()
}
