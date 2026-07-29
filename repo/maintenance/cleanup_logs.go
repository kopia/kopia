package maintenance

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// LogRetentionOptions provides options for logs retention.
type LogRetentionOptions struct {
	MaxTotalSize int64            `json:"maxTotalSize"`
	MaxCount     int              `json:"maxCount"`
	MaxAge       time.Duration    `json:"maxAge"`
	DryRun       bool             `json:"-"`
	TimeFunc     func() time.Time `json:"-"`
}

// OrDefault returns default LogRetentionOptions.
func (o LogRetentionOptions) OrDefault() LogRetentionOptions {
	if o.MaxCount == 0 && o.MaxAge == 0 && o.MaxTotalSize == 0 {
		return defaultLogRetention()
	}

	return o
}

// defaultLogRetention returns CleanupLogsOptions applied by default during maintenance.
func defaultLogRetention() LogRetentionOptions {
	//nolint:mnd
	return LogRetentionOptions{
		MaxTotalSize: 1 << 30,             // keep no more than 1 GiB logs
		MaxAge:       30 * 24 * time.Hour, // no more than 30 days of data
		MaxCount:     10000,               // no more than 10K logs
	}
}

// CleanupLogs deletes old logs blobs beyond certain age, total size or count.
func CleanupLogs(ctx context.Context, rep repo.DirectRepositoryWriter, opt LogRetentionOptions) (*maintenancestats.CleanupLogsStats, error) {
	ctx = contentlog.WithParams(ctx,
		logparam.String("span:cleanup-logs", contentlog.RandomSpanID()))

	log := rep.LogManager().NewLogger("maintenance-cleanup-logs")

	if opt.TimeFunc == nil {
		opt.TimeFunc = clock.Now
	}

	allLogBlobs, err := blob.ListAllBlobs(ctx, rep.BlobStorage(), repodiag.LogBlobPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "error listing logs")
	}

	// sort by time so that most recent are first
	sort.Slice(allLogBlobs, func(i, j int) bool {
		return allLogBlobs[i].Timestamp.After(allLogBlobs[j].Timestamp)
	})

	var retainedSize uint64

	deletePosition := len(allLogBlobs)

	for i, bm := range allLogBlobs {
		bmlen := maintenancestats.ToUint64(bm.Length)

		if opt.MaxTotalSize > 0 && retainedSize+bmlen > uint64(opt.MaxTotalSize) {
			deletePosition = i
			break
		}

		if i >= opt.MaxCount && opt.MaxCount > 0 {
			deletePosition = i
			break
		}

		if age := opt.TimeFunc().Sub(bm.Timestamp); age > opt.MaxAge && opt.MaxAge != 0 {
			deletePosition = i
			break
		}

		retainedSize += bmlen
	}

	toDelete := allLogBlobs[deletePosition:]

	var toDeleteSize uint64
	for _, bm := range toDelete {
		toDeleteSize += maintenancestats.ToUint64(bm.Length)
	}

	result := &maintenancestats.CleanupLogsStats{
		RetainedBlobCount: maintenancestats.ToUint64(deletePosition),
		RetainedBlobSize:  retainedSize,
		ToDeleteBlobCount: maintenancestats.ToUint64(len(toDelete)),
		ToDeleteBlobSize:  toDeleteSize,
		DeletedBlobCount:  0,
		DeletedBlobSize:   0,
	}

	contentlog.Log1(ctx, log, "Clean up logs", result)

	if !opt.DryRun {
		// Under a storage-level retention policy (e.g. S3 Object Lock in
		// compliance mode, or a bucket default retention that applies to
		// every object) a log blob can still be inside its retention window
		// and is undeletable by anyone. Log blobs are pure diagnostics, so
		// keeping one for another cycle costs nothing but storage - failing
		// the whole maintenance run's exit code for it is wrong. Note this
		// is not a corner case with our defaults: log retention is 30 days
		// and a typical Object Lock retention is also 30 days, so the two
		// boundaries coincide.
		objectLockEnabled := rep.ContentManager().IsObjectLockEnabled()

		var deletedCount, retainedByLock uint64

		var deletedSize uint64

		for _, bm := range toDelete {
			if err := rep.BlobStorage().DeleteBlob(ctx, bm.BlobID); err != nil {
				if objectLockEnabled {
					retainedByLock++

					contentlog.Log2(ctx, log,
						"could not delete log blob (object lock enabled), leaving it for a future maintenance cycle",
						blobparam.BlobID("blobID", bm.BlobID),
						logparam.Error("error", err))

					continue
				}

				return nil, errors.Wrapf(err, "error deleting log %v", bm.BlobID)
			}

			deletedCount++
			deletedSize += maintenancestats.ToUint64(bm.Length)
		}

		if retainedByLock > 0 {
			contentlog.Log1(ctx, log,
				"some log blobs could not be deleted because of object lock retention",
				logparam.UInt64("retainedByLock", retainedByLock))
		}

		result.DeletedBlobCount = deletedCount
		result.DeletedBlobSize = deletedSize
	}

	return result, nil
}
