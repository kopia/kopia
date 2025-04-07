package maintenance

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/units"
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
						log(ctx).Infof("  deleted %v unreferenced blobs (%v)", cnt, units.BytesString(del))
					}
				}

				return nil
			})
		}
	}

	// iterate unreferenced blobs and count them + optionally send to the channel to be deleted
	log(ctx).Info("Looking for unreferenced blobs...")

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
	if err := rep.ContentManager().IterateUnreferencedBlobs(ctx, prefixes, opt.Parallel, func(bm blob.Metadata) error {
		if bm.Timestamp.After(cutoffTime) {
			log(ctx).Debugf("  preserving %v because it was created after maintenance started", bm.BlobID)
			return nil
		}

		if age := cutoffTime.Sub(bm.Timestamp); age < safety.BlobDeleteMinAge {
			log(ctx).Debugf("  preserving %v because it's too new (age: %v<%v)", bm.BlobID, age, safety.BlobDeleteMinAge)
			return nil
		}

		sid := content.SessionIDFromBlobID(bm.BlobID)
		if s, ok := activeSessions[sid]; ok {
			if age := cutoffTime.Sub(s.CheckpointTime); age < safety.SessionExpirationAge {
				log(ctx).Debugf("  preserving %v because it's part of an active session (%v)", bm.BlobID, sid)
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
	log(ctx).Debugf("Found %v blobs to delete (%v)", unreferencedCount, units.BytesString(unreferencedSize))

	// wait for all delete workers to finish.
	if err := eg.Wait(); err != nil {
		return 0, errors.Wrap(err, "worker error")
	}

	if opt.DryRun {
		return int(unreferencedCount), nil
	}

	del, cnt := deleted.Approximate()

	log(ctx).Infof("Deleted total %v unreferenced blobs (%v)", del, units.BytesString(cnt))

	return int(del), nil
}
