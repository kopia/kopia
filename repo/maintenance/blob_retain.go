package maintenance

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenancestats"
)

const parallelBlobRetainCPUMultiplier = 2

const minRetentionMaintenanceDiff = time.Duration(24) * time.Hour

// ExtendBlobRetentionTimeOptions provides options for extending blob retention algorithm.
type ExtendBlobRetentionTimeOptions struct {
	Parallel int
	DryRun   bool
}

// extendBlobRetentionTime extends the retention time of all relevant blobs managed by storage engine with Object Locking enabled.
func extendBlobRetentionTime(ctx context.Context, rep repo.DirectRepositoryWriter, opt ExtendBlobRetentionTimeOptions) (*maintenancestats.ExtendBlobRetentionStats, error) {
	ctx = contentlog.WithParams(ctx,
		logparam.String("span:blob-retain", contentlog.RandomSpanID()))
	log := rep.LogManager().NewLogger("maintenance-blob-retain")

	blobCfg, err := rep.FormatManager().BlobCfgBlob(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "blob configuration")
	}

	if !blobCfg.IsRetentionEnabled() {
		// Blob retention is disabled
		contentlog.Log(ctx, log, "Object lock retention is disabled.")

		return nil, nil
	}

	const extendQueueSize = 100

	extend := make(chan blob.Metadata, extendQueueSize)
	extendOpts := blob.ExtendOptions{
		RetentionMode:   blobCfg.RetentionMode,
		RetentionPeriod: blobCfg.RetentionPeriod,
	}

	var (
		wg                                   errgroup.Group
		extendedCount, toExtend, failedCount atomic.Uint32
	)

	if opt.Parallel == 0 {
		opt.Parallel = runtime.NumCPU() * parallelBlobRetainCPUMultiplier
	}

	if !opt.DryRun {
		// start goroutines to extend blob retention as they come.
		for range opt.Parallel {
			wg.Go(func() error {
				for bm := range extend {
					if err1 := rep.BlobStorage().ExtendBlobRetention(ctx, bm.BlobID, extendOpts); err1 != nil {
						contentlog.Log2(ctx, log,
							"Failed to extend blob",
							blobparam.BlobID("blobID", bm.BlobID),
							logparam.Error("error", err1))

						failedCount.Add(1)

						continue
					}

					if currentCount := extendedCount.Add(1); currentCount%100 == 0 {
						contentlog.Log1(ctx, log, "extended blobs", logparam.UInt32("count", currentCount))
					}
				}

				return nil
			})
		}
	}

	// iterate all relevant (active, extendable) blobs and count them + optionally send to the channel to be extended
	contentlog.Log(ctx, log, "Extending retention time for blobs...")

	err = blob.IterateAllPrefixesInParallel(ctx, opt.Parallel, rep.BlobStorage(), repo.GetLockingStoragePrefixes(), func(bm blob.Metadata) error {
		if !opt.DryRun {
			extend <- bm
		}

		toExtend.Add(1)

		return nil
	})

	close(extend)

	contentlog.Log1(ctx, log, "Found blobs to extend", logparam.UInt32("count", toExtend.Load()))

	wg.Wait() // wait for all extend workers to finish.

	if count := failedCount.Load(); count > 0 {
		return nil, errors.Errorf("Failed to extend %v blobs", count)
	}

	if err != nil {
		return nil, errors.Wrap(err, "error iterating packs")
	}

	result := &maintenancestats.ExtendBlobRetentionStats{
		ToExtendBlobCount: toExtend.Load(),
		ExtendedBlobCount: extendedCount.Load(),
		RetentionPeriod:   extendOpts.RetentionPeriod.String(),
	}

	contentlog.Log1(ctx, log, "Extended retention time for blobs", result)

	if opt.DryRun {
		return result, nil
	}

	return result, nil
}

// CheckExtendRetention verifies if extension can be enabled due to maintenance and blob parameters.
func CheckExtendRetention(ctx context.Context, blobCfg format.BlobStorageConfiguration, p *Params) error {
	if !p.ExtendObjectLocks {
		return nil
	}

	if !p.FullCycle.Enabled {
		userLog(ctx).Warn("Object Lock extension will not function because Full-Maintenance is disabled")
	}

	if blobCfg.RetentionPeriod > 0 && blobCfg.RetentionPeriod-p.FullCycle.Interval < minRetentionMaintenanceDiff {
		return errors.Errorf("The repo RetentionPeriod must be %v greater than the Full Maintenance interval %v %v", minRetentionMaintenanceDiff, blobCfg.RetentionPeriod, p.FullCycle.Interval)
	}

	return nil
}
