package snapshotfs

import (
	"context"
	"sync/atomic"

	"github.com/kopia/kopia/snapshot"
)

type scanResults struct {
	numFiles      int
	totalFileSize int64
}

func (e *scanResults) Error(context.Context, string, error, bool) {}

func (e *scanResults) Processing(context.Context, string) {}

//nolint:revive
func (e *scanResults) Stats(ctx context.Context, s *snapshot.Stats, includedFiles, excludedFiles SampleBuckets, excludedDirs []string, final bool) {
	if final {
		e.numFiles = int(atomic.LoadInt32(&s.TotalFileCount))
		e.totalFileSize = atomic.LoadInt64(&s.TotalFileSize)
	}
}

var _ EstimateProgress = (*scanResults)(nil)
