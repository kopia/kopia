package maintenance

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
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

	allLogBlobs, err := blob.ListAllBlobs(ctx, rep.BlobStorage(), "_")
	if err != nil {
		return nil, errors.Wrap(err, "error listing logs")
	}

	// sort by time so that most recent are first
	sort.Slice(allLogBlobs, func(i, j int) bool {
		return allLogBlobs[i].Timestamp.After(allLogBlobs[j].Timestamp)
	})

	var retainedSize int64

	deletePosition := len(allLogBlobs)

	for i, bm := range allLogBlobs {
		retainedSize += bm.Length

		if retainedSize > opt.MaxTotalSize && opt.MaxTotalSize > 0 {
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
	}

	toDelete := allLogBlobs[deletePosition:]

	var toDeleteSize int64
	for _, bm := range toDelete {
		toDeleteSize += bm.Length
	}

	result := &maintenancestats.CleanupLogsStats{
		RetainedBlobCount: deletePosition,
		RetainedBlobSize:  retainedSize,
		ToDeleteBlobCount: len(toDelete),
		ToDeleteBlobSize:  toDeleteSize,
		DeletedBlobCount:  0,
		DeletedBlobSize:   0,
	}

	contentlog.Log1(ctx, log, "Clean up logs", result)

	if !opt.DryRun {
		for _, bm := range toDelete {
			if err := rep.BlobStorage().DeleteBlob(ctx, bm.BlobID); err != nil {
				return nil, errors.Wrapf(err, "error deleting log %v", bm.BlobID)
			}
		}

		result.DeletedBlobCount = result.ToDeleteBlobCount
		result.DeletedBlobSize = result.ToDeleteBlobSize
	}

	return result, nil
}
