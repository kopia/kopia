package snapshotfs

import (
	"context"
	"sync/atomic"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
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

// scanDirectory computes the number of files and their total size in a given directory recursively descending
// into subdirectories. The scan teminates early as soon as the provided context is canceled.
func (u *Uploader) scanDirectory(ctx context.Context, dir fs.Directory, policyTree *policy.Tree) (scanResults, error) {
	var res scanResults

	if u.disableEstimation {
		return res, nil
	}

	err := Estimate(ctx, dir, policyTree, &res, 1)

	return res, err
}
