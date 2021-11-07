package snapshotfs

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

// SampleBucket keeps track of count and total size of files above in certain size range and
// includes small number of examples of such files.
type SampleBucket struct {
	MinSize   int64    `json:"minSize"`
	Count     int      `json:"count"`
	TotalSize int64    `json:"totalSize"`
	Examples  []string `json:"examples,omitempty"`
}

func (b *SampleBucket) add(fname string, size int64, maxExamplesPerBucket int) {
	b.Count++
	b.TotalSize += size

	if len(b.Examples) < maxExamplesPerBucket {
		b.Examples = append(b.Examples, fmt.Sprintf("%v - %v", fname, units.BytesStringBase10(size)))
	}
}

// SampleBuckets is a collection of buckets for interesting file sizes sorted in descending order.
type SampleBuckets []*SampleBucket

func (b SampleBuckets) add(fname string, size int64, maxExamplesPerBucket int) {
	for _, bucket := range b {
		if size >= bucket.MinSize {
			bucket.add(fname, size, maxExamplesPerBucket)
			break
		}
	}
}

func makeBuckets() SampleBuckets {
	return SampleBuckets{
		&SampleBucket{MinSize: 1e15},
		&SampleBucket{MinSize: 1e14},
		&SampleBucket{MinSize: 1e13},
		&SampleBucket{MinSize: 1e12},
		&SampleBucket{MinSize: 1e11},
		&SampleBucket{MinSize: 1e10},
		&SampleBucket{MinSize: 1e9},
		&SampleBucket{MinSize: 1e8},
		&SampleBucket{MinSize: 1e7},
		&SampleBucket{MinSize: 1e6},
		&SampleBucket{MinSize: 1e5},
		&SampleBucket{MinSize: 1e4},
		&SampleBucket{MinSize: 1e3},
		&SampleBucket{MinSize: 0},
	}
}

// EstimateProgress must be provided by the caller of Estimate to report results.
type EstimateProgress interface {
	Processing(ctx context.Context, dirname string)
	Error(ctx context.Context, filename string, err error, isIgnored bool)
	Stats(ctx context.Context, s *snapshot.Stats, includedFiles, excludedFiles SampleBuckets, excludedDirs []string, final bool)
}

// Estimate walks the provided directory tree and invokes provided progress callback as it discovers
// items to be snapshotted.
func Estimate(ctx context.Context, rep repo.Repository, entry fs.Directory, policyTree *policy.Tree, progress EstimateProgress, maxExamplesPerBucket int) error {
	stats := &snapshot.Stats{}
	ed := []string{}
	ib := makeBuckets()
	eb := makeBuckets()

	// report final stats just before returning
	defer func() {
		progress.Stats(ctx, stats, ib, eb, ed, true)
	}()

	onIgnoredFile := func(ctx context.Context, relativePath string, e fs.Entry, pol *policy.Tree) {
		if e.IsDir() {
			if len(ed) < maxExamplesPerBucket {
				ed = append(ed, relativePath)
			}

			stats.ExcludedDirCount++

			estimateLog(ctx).Debugw("excluded directory", "path", relativePath)
		} else {
			estimateLog(ctx).Debugw("excluded file", "path", relativePath, "size", units.BytesStringBase10(e.Size()))
			stats.ExcludedFileCount++
			stats.ExcludedTotalFileSize += e.Size()
			eb.add(relativePath, e.Size(), maxExamplesPerBucket)
		}
	}

	entry = ignorefs.New(entry, policyTree, ignorefs.ReportIgnoredFiles(onIgnoredFile))

	return estimate(ctx, ".", entry, policyTree, stats, ib, eb, &ed, progress, maxExamplesPerBucket)
}

func estimate(ctx context.Context, relativePath string, entry fs.Entry, policyTree *policy.Tree, stats *snapshot.Stats, ib, eb SampleBuckets, ed *[]string, progress EstimateProgress, maxExamplesPerBucket int) error {
	// see if the context got canceled
	select {
	case <-ctx.Done():
		// nolint:wrapcheck
		return ctx.Err()

	default:
	}

	switch entry := entry.(type) {
	case fs.Directory:
		stats.TotalDirectoryCount++

		progress.Processing(ctx, relativePath)

		children, err := entry.Readdir(ctx)
		if err != nil {
			isIgnored := policyTree.EffectivePolicy().ErrorHandlingPolicy.IgnoreDirectoryErrors.OrDefault(false)

			if isIgnored {
				stats.IgnoredErrorCount++
			} else {
				stats.ErrorCount++
			}

			progress.Error(ctx, relativePath, err, isIgnored)
		} else {
			for _, child := range children {
				if err := estimate(ctx, filepath.Join(relativePath, child.Name()), child, policyTree.Child(child.Name()), stats, ib, eb, ed, progress, maxExamplesPerBucket); err != nil {
					return err
				}
			}
		}

		progress.Stats(ctx, stats, ib, eb, *ed, false)

	case fs.File:
		ib.add(relativePath, entry.Size(), maxExamplesPerBucket)
		stats.TotalFileCount++
		stats.TotalFileSize += entry.Size()
	}

	return nil
}
