package cli

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/content/indexblob"
)

type commandIndexRecover struct {
	blobIDs       []string
	blobPrefixes  []string
	commit        bool
	ignoreErrors  bool
	parallel      int
	deleteIndexes bool

	svc appServices
}

func (c *commandIndexRecover) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("recover", "Recover indexes from pack blobs")
	cmd.Flag("blob-prefixes", "Prefixes of pack blobs to recover from (default=all packs)").StringsVar(&c.blobPrefixes)
	cmd.Flag("blobs", "Names of pack blobs to recover from (default=all packs)").StringsVar(&c.blobIDs)
	cmd.Flag("parallel", "Recover parallelism").Default("1").IntVar(&c.parallel)
	cmd.Flag("ignore-errors", "Ignore errors when recovering").BoolVar(&c.ignoreErrors)
	cmd.Flag("delete-indexes", "Delete all indexes before recovering").BoolVar(&c.deleteIndexes)
	cmd.Flag("commit", "Commit recovered content").BoolVar(&c.commit)
	cmd.Action(svc.directRepositoryWriteAction(c.run))

	c.svc = svc
}

func (c *commandIndexRecover) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	c.svc.advancedCommand(ctx)

	var (
		processedBlobCount    atomic.Int32
		recoveredContentCount atomic.Int32
	)

	defer func() {
		if recoveredContentCount.Load() == 0 {
			log(ctx).Info("No contents recovered.")
			return
		}

		if !c.commit {
			log(ctx).Infof("Found %v contents to recover from %v blobs, but not committed. Re-run with --commit", recoveredContentCount.Load(), processedBlobCount.Load())
		} else {
			log(ctx).Infof("Recovered %v contents from %v.", recoveredContentCount.Load(), processedBlobCount.Load())
		}
	}()

	if c.deleteIndexes {
		if err := rep.BlobReader().ListBlobs(ctx, indexblob.V0IndexBlobPrefix, func(bm blob.Metadata) error {
			if c.commit {
				log(ctx).Infof("deleting old index blob: %v", bm.BlobID)
				return errors.Wrap(rep.BlobStorage().DeleteBlob(ctx, bm.BlobID), "error deleting index blob")
			}

			log(ctx).Infof("would delete old index: %v (pass --commit to approve)", bm.BlobID)
			return nil
		}); err != nil {
			return errors.Wrap(err, "error deleting old indexes")
		}
	}

	if len(c.blobIDs) == 0 {
		var prefixes []blob.ID

		if len(c.blobPrefixes) > 0 {
			for _, p := range c.blobPrefixes {
				prefixes = append(prefixes, blob.ID(p))
			}
		} else {
			prefixes = content.PackBlobIDPrefixes
		}

		return c.recoverIndexesFromAllPacks(ctx, rep, prefixes, &processedBlobCount, &recoveredContentCount)
	}

	for _, packFile := range c.blobIDs {
		if err := c.recoverIndexFromSinglePackFile(ctx, rep, blob.ID(packFile), 0, &processedBlobCount, &recoveredContentCount); err != nil && !c.ignoreErrors {
			return errors.Wrapf(err, "error recovering index from %v", packFile)
		}
	}

	return nil
}

func (c *commandIndexRecover) recoverIndexesFromAllPacks(ctx context.Context, rep repo.DirectRepositoryWriter, prefixes []blob.ID, processedBlobCount, recoveredContentCount *atomic.Int32) error {
	var (
		discoveredBlobCount  atomic.Int32
		discoveringBlobCount atomic.Int32
		tt                   timetrack.Throttle
	)

	// recover indexes from all pack blobs in parallel.
	// this is actually quite fast since we typically need to read only 8KB from each blob.
	eg, ctx := errgroup.WithContext(ctx)

	go func() {
		for _, prefix := range prefixes {
			//nolint:errcheck
			rep.BlobStorage().ListBlobs(ctx, prefix, func(_ blob.Metadata) error {
				discoveringBlobCount.Add(1)
				return nil
			})
		}

		discoveredBlobCount.Store(discoveringBlobCount.Load())
	}()

	est := timetrack.Start()

	blobCh := make(chan blob.Metadata)

	// goroutine to populate blob metadata into a channel.
	eg.Go(func() error {
		defer close(blobCh)

		for _, prefix := range prefixes {
			if err := rep.BlobStorage().ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
				blobCh <- bm
				return nil
			}); err != nil {
				return errors.Wrapf(err, "error listing blobs with prefix %q", prefix)
			}
		}

		return nil
	})

	// N goroutines to recover from incoming blobs.
	for worker := range c.parallel {
		eg.Go(func() error {
			cnt := 0

			for bm := range blobCh {
				finishedBlobs := processedBlobCount.Load()

				log(ctx).Debugf("worker %v got %v", worker, cnt)

				cnt++

				if tt.ShouldOutput(time.Second) {
					if disc := discoveredBlobCount.Load(); disc > 0 {
						e, ok := est.Estimate(float64(finishedBlobs), float64(disc))
						if ok {
							log(ctx).Infof("Recovered %v index entries from %v/%v blobs (%.1f %%) %v remaining %v ETA",
								recoveredContentCount.Load(),
								finishedBlobs,
								disc,
								e.PercentComplete,
								e.Remaining,
								formatTimestamp(e.EstimatedEndTime))
						}
					} else {
						log(ctx).Infof("Recovered %v index entries from %v blobs, estimating time remaining... (found %v blobs)",
							recoveredContentCount.Load(),
							finishedBlobs,
							discoveringBlobCount.Load())
					}
				}

				if err := c.recoverIndexFromSinglePackFile(ctx, rep, bm.BlobID, bm.Length, processedBlobCount, recoveredContentCount); err != nil {
					return errors.Wrapf(err, "error recovering from %v", bm.BlobID)
				}
			}

			return nil
		})
	}

	return errors.Wrap(eg.Wait(), "recovering indexes")
}

func (c *commandIndexRecover) recoverIndexFromSinglePackFile(ctx context.Context, rep repo.DirectRepositoryWriter, blobID blob.ID, length int64, processedBlobCount, recoveredContentCount *atomic.Int32) error {
	log(ctx).Debugf("recovering from %v", blobID)

	recovered, err := rep.ContentManager().RecoverIndexFromPackBlob(ctx, blobID, length, c.commit)
	if err != nil {
		if c.ignoreErrors {
			return nil
		}

		return errors.Wrapf(err, "unable to recover index from %v", blobID)
	}

	recoveredContentCount.Add(int32(len(recovered))) //nolint:gosec
	processedBlobCount.Add(1)
	log(ctx).Debugf("Recovered %v entries from %v (commit=%v)", len(recovered), blobID, c.commit)

	return nil
}
