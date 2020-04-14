package maintenance

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo/blob"
)

// defaultBlobGCMinAge is a default MinAge for blob GC.
// We're setting it to 2 hours to accommodate the fact that every repository
// will periodically flush its indexes more frequently than 1/hour.
const defaultBlobGCMinAge = 2 * time.Hour

// DeleteUnreferencedBlobsOptions provides option for blob garbage collection algorithm.
type DeleteUnreferencedBlobsOptions struct {
	Parallel int
	Prefix   blob.ID
	MinAge   time.Duration
	DryRun   bool
}

// DeleteUnreferencedBlobs deletes old blobs that are no longer referenced by index entries.
func DeleteUnreferencedBlobs(ctx context.Context, rep MaintainableRepository, opt DeleteUnreferencedBlobsOptions) (int, error) {
	if opt.Parallel == 0 {
		opt.Parallel = 16
	}

	if opt.MinAge == 0 {
		opt.MinAge = defaultBlobGCMinAge
	}

	const deleteQueueSize = 100

	var unreferenced, deleted stats.CountSum

	var eg errgroup.Group

	unused := make(chan blob.Metadata, deleteQueueSize)

	if !opt.DryRun {
		// start goroutines to delete blobs as they come.
		for i := 0; i < opt.Parallel; i++ {
			eg.Go(func() error {
				for bm := range unused {
					if err := rep.BlobStorage().DeleteBlob(ctx, bm.BlobID); err != nil {
						return errors.Wrapf(err, "unable to delete blob %q", bm.BlobID)
					}
					cnt, del := deleted.Add(bm.Length)
					if cnt%100 == 0 {
						log(ctx).Infof("  deleted %v unreferenced blobs (%v)", cnt, units.BytesStringBase10(del))
					}
				}

				return nil
			})
		}
	}

	// iterate unreferenced blobs and count them + optionally send to the channel to be deleted
	log(ctx).Infof("Looking for unreferenced blobs...")

	var prefixes []blob.ID
	if p := opt.Prefix; p != "" {
		prefixes = append(prefixes, p)
	}

	if err := rep.ContentManager().IterateUnreferencedBlobs(ctx, prefixes, opt.Parallel, func(bm blob.Metadata) error {
		if age := rep.Time().Sub(bm.Timestamp); age < opt.MinAge {
			log(ctx).Debugf("  preserving %v because it's too new (age: %v)", bm.BlobID, age)
			return nil
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
	log(ctx).Debugf("Found %v blobs to delete (%v)", unreferencedCount, units.BytesStringBase10(unreferencedSize))

	// wait for all delete workers to finish.
	if err := eg.Wait(); err != nil {
		return 0, err
	}

	if opt.DryRun {
		return int(unreferencedCount), nil
	}

	del, cnt := deleted.Approximate()

	log(ctx).Infof("Deleted total %v unreferenced blobs (%v)", del, units.BytesStringBase10(cnt))

	return int(del), nil
}
