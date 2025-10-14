package maintenance

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobparam"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

// DeleteUnreferencedBlobsOptions provides option for blob garbage collection algorithm.
type DeleteUnreferencedBlobsOptions struct {
	Parallel     int
	Prefix       blob.ID
	DryRun       bool
	NotAfterTime time.Time
}

// DeleteUnreferencedBlobs deletes o was created after maintenance startederenced by index entries.
//
//nolint:gocyclo,funlen
func DeleteUnreferencedBlobs(ctx context.Context, rep repo.DirectRepositoryWriter, opt DeleteUnreferencedBlobsOptions, safety SafetyParameters) (int, error) {
	ctx = contentlog.WithParams(ctx,
		logparam.String("span:blob-gc", contentlog.RandomSpanID()))

	log := rep.LogManager().NewLogger("maintenance-blob-gc")

	if opt.Parallel == 0 {
		opt.Parallel = 16
	}

	const deleteQueueSize = 100

	var unreferenced, deleted stats.CountSum

	var eg errgroup.Group

	unused := make(chan blob.Metadata, deleteQueueSize)

	if !opt.DryRun {
		// start goroutines to delete blobs as they come.
		for range opt.Parallel {
			eg.Go(func() error {
				for bm := range unused {
					if err := rep.BlobStorage().DeleteBlob(ctx, bm.BlobID); err != nil {
						return errors.Wrapf(err, "unable to delete blob %q", bm.BlobID)
					}

					cnt, del := deleted.Add(bm.Length)
					if cnt%100 == 0 {
						contentlog.Log2(ctx, log, "deleted unreferenced blobs", logparam.UInt32("count", cnt), logparam.Int64("bytes", del))
					}
				}

				return nil
			})
		}
	}

	// iterate unreferenced blobs and count them + optionally send to the channel to be deleted
	contentlog.Log(ctx, log, "Looking for unreferenced blobs...")

	var prefixes []blob.ID
	if p := opt.Prefix; p != "" {
		prefixes = append(prefixes, p)
	} else {
		prefixes = append(prefixes, content.PackBlobIDPrefixRegular, content.PackBlobIDPrefixSpecial, content.BlobIDPrefixSession)
	}

	activeSessions, err := rep.ContentManager().ListActiveSessions(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "unable to load active sessions")
	}

	cutoffTime := opt.NotAfterTime
	if cutoffTime.IsZero() {
		cutoffTime = rep.Time()
	}

	// move the cutoff time a bit forward, because on Windows clock does not reliably move forward so we may end
	// up not deleting some blobs - this only really affects tests, since BlobDeleteMinAge provides real
	// protection here.
	const cutoffTimeSlack = 1 * time.Second

	cutoffTime = cutoffTime.Add(cutoffTimeSlack)

	// iterate all pack blobs + session blobs and keep ones that are too young or
	// belong to alive sessions.
	if err := rep.ContentManager().IterateUnreferencedPacks(ctx, prefixes, opt.Parallel, func(bm blob.Metadata) error {
		if bm.Timestamp.After(cutoffTime) {
			contentlog.Log3(ctx, log,
				"preserving blob - after cutoff time",
				blobparam.BlobID("blobID", bm.BlobID),
				logparam.Time("cutoffTime", cutoffTime),
				logparam.Time("timestamp", bm.Timestamp))
			return nil
		}

		if age := cutoffTime.Sub(bm.Timestamp); age < safety.BlobDeleteMinAge {
			contentlog.Log2(ctx, log,
				"preserving blob - below min age",
				blobparam.BlobID("blobID", bm.BlobID),
				logparam.Duration("age", age))
			return nil
		}

		sid := content.SessionIDFromBlobID(bm.BlobID)
		if s, ok := activeSessions[sid]; ok {
			if age := cutoffTime.Sub(s.CheckpointTime); age < safety.SessionExpirationAge {
				contentlog.Log2(ctx, log,
					"preserving blob - part of active session",
					blobparam.BlobID("blobID", bm.BlobID),
					logparam.String("sessionID", string(sid)))
				return nil
			}
		}

		unreferenced.Add(bm.Length)

		if !opt.DryRun {
			unused <- bm
		}

		return nil
	}); err != nil {
		return 0, errors.Wrap(err, "error looking for unreferenced blobs")
	}

	close(unused)

	unreferencedCount, unreferencedSize := unreferenced.Approximate()
	contentlog.Log2(ctx, log,
		"Found blobs to delete",
		logparam.UInt32("count", unreferencedCount),
		logparam.Int64("bytes", unreferencedSize))

	// wait for all delete workers to finish.
	if err := eg.Wait(); err != nil {
		return 0, errors.Wrap(err, "worker error")
	}

	if opt.DryRun {
		return int(unreferencedCount), nil
	}

	del, cnt := deleted.Approximate()

	contentlog.Log2(ctx, log,
		"Deleted total blobs",
		logparam.UInt32("count", del),
		logparam.Int64("bytes", cnt))

	return int(del), nil
}
