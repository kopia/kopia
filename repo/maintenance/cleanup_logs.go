package maintenance

import (
	"context"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
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
func CleanupLogs(ctx context.Context, rep repo.DirectRepositoryWriter, opt LogRetentionOptions) ([]blob.Metadata, error) {
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

	var totalSize int64

	deletePosition := len(allLogBlobs)

	for i, bm := range allLogBlobs {
		totalSize += bm.Length

		if totalSize > opt.MaxTotalSize && opt.MaxTotalSize > 0 {
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

	log(ctx).Debugf("Keeping %v logs of total size %v", deletePosition, units.BytesString(totalSize))

	if !opt.DryRun {
		for _, bm := range toDelete {
			if err := rep.BlobStorage().DeleteBlob(ctx, bm.BlobID); err != nil {
				return nil, errors.Wrapf(err, "error deleting log %v", bm.BlobID)
			}
		}
	}

	return toDelete, nil
}
