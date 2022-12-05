package repo

import (
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/metrics"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/hashing"
)

// immutableServerRepositoryParameters contains immutable parameters shared between HTTP and GRPC clients.
type immutableServerRepositoryParameters struct {
	h               hashing.HashFunc
	objectFormat    format.ObjectFormat
	cliOpts         ClientOptions
	metricsRegistry *metrics.Registry
	contentCache    *cache.PersistentCache
	beforeFlush     []RepositoryWriterCallback

	*refCountedCloser
}

// Metrics provides access to the metrics registry.
func (r *immutableServerRepositoryParameters) Metrics() *metrics.Registry {
	return r.metricsRegistry
}

func (r *immutableServerRepositoryParameters) ClientOptions() ClientOptions {
	return r.cliOpts
}
