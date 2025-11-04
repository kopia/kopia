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
	"github.com/kopia/kopia/repo/maintenancestats"
)

// DeleteUnreferencedPacksOptions provides option for pack garbage collection algorithm.
type DeleteUnreferencedPacksOptions struct {
	Parallel     int
	Prefix       blob.ID
	DryRun       bool
	NotAfterTime time.Time
}

// DeleteUnreferencedPacks deletes pack blobs that are unreferenced by index entries.
//
//nolint:gocyclo,funlen
func DeleteUnreferencedPacks(ctx context.Context, rep repo.DirectRepositoryWriter, opt DeleteUnreferencedPacksOptions, safety SafetyParameters) (*maintenancestats.DeleteUnreferencedPacksStats, error) {
	ctx = contentlog.WithParams(ctx,
		logparam.String("span:pack-gc", contentlog.RandomSpanID()))

	log := rep.LogManager().NewLogger("maintenance-pack-gc")

	if opt.Parallel == 0 {
		opt.Parallel = 16
	}

	const deleteQueueSize = 100

	var unreferenced, deleted, retained stats.CountSum

	var eg errgroup.Group

	unused := make(chan blob.Metadata, deleteQueueSize)

	if !opt.DryRun {
		// start goroutines to delete packs as they come.
		for range opt.Parallel {
			eg.Go(func() error {
				for bm := range unused {
					if err := rep.BlobStorage().DeleteBlob(ctx, bm.BlobID); err != nil {
						return errors.Wrapf(err, "unable to delete pack blob %q", bm.BlobID)
					}

					cnt, del := deleted.Add(bm.Length)
					if cnt%100 == 0 {
						contentlog.Log2(ctx, log, "deleted unreferenced pack blobs", logparam.UInt32("count", cnt), logparam.Int64("bytes", del))
					}
				}

				return nil
			})
		}
	}

	// iterate unreferenced packs and count them + optionally send to the channel to be deleted
	contentlog.Log(ctx, log, "Looking for unreferenced pack blobs...")

	var prefixes []blob.ID
	if p := opt.Prefix; p != "" {
		prefixes = append(prefixes, p)
	} else {
		prefixes = append(prefixes, content.PackBlobIDPrefixRegular, content.PackBlobIDPrefixSpecial, content.BlobIDPrefixSession)
	}

	activeSessions, err := rep.ContentManager().ListActiveSessions(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load active sessions")
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
			retained.Add(bm.Length)

			contentlog.Log3(ctx, log,
				"preserving pack - after cutoff time",
				blobparam.BlobID("blobID", bm.BlobID),
				logparam.Time("cutoffTime", cutoffTime),
				logparam.Time("timestamp", bm.Timestamp))
			return nil
		}

		if age := cutoffTime.Sub(bm.Timestamp); age < safety.PackDeleteMinAge {
			retained.Add(bm.Length)

			contentlog.Log2(ctx, log,
				"preserving pack - below min age",
				blobparam.BlobID("blobID", bm.BlobID),
				logparam.Duration("age", age))
			return nil
		}

		sid := content.SessionIDFromBlobID(bm.BlobID)
		if s, ok := activeSessions[sid]; ok {
			if age := cutoffTime.Sub(s.CheckpointTime); age < safety.SessionExpirationAge {
				retained.Add(bm.Length)

				contentlog.Log2(ctx, log,
					"preserving pack - part of active session",
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
		return nil, errors.Wrap(err, "error looking for unreferenced pack blobs")
	}

	close(unused)

	unreferencedCount, unreferencedSize := unreferenced.Approximate()
	retainedCount, retainedSize := retained.Approximate()

	result := &maintenancestats.DeleteUnreferencedPacksStats{
		UnreferencedPackCount: unreferencedCount,
		UnreferencedTotalSize: unreferencedSize,
		RetainedPackCount:     retainedCount,
		RetainedTotalSize:     retainedSize,
		DeletedPackCount:      0,
		DeletedTotalSize:      0,
	}

	contentlog.Log1(ctx, log, "Found unreferenced pack blobs to delete", result)

	// wait for all delete workers to finish.
	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "worker error")
	}

	if opt.DryRun {
		return result, nil
	}

	deletedCount, deletedSize := deleted.Approximate()
	result.DeletedPackCount = deletedCount
	result.DeletedTotalSize = deletedSize

	contentlog.Log1(ctx, log, "Completed deleting unreferenced pack blobs", result)

	return result, nil
}
