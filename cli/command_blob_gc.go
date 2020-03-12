package cli

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

var (
	blobGarbageCollectCommand       = blobCommands.Command("gc", "Garbage-collect unused blobs")
	blobGarbageCollectCommandDelete = blobGarbageCollectCommand.Flag("delete", "Whether to delete unused blobs").String()
	blobGarbageCollectParallel      = blobGarbageCollectCommand.Flag("parallel", "Number of parallel blob scans").Default("16").Int()
	blobGarbageCollectMinAge        = blobGarbageCollectCommand.Flag("min-age", "Garbage-collect blobs with minimum age").Default("24h").Duration()
)

func runBlobGarbageCollectCommand(ctx context.Context, rep *repo.Repository) error {
	const deleteQueueSize = 100

	var unreferenced, deleted stats.CountSum

	var eg errgroup.Group

	unused := make(chan blob.Metadata, deleteQueueSize)

	if *blobGarbageCollectCommandDelete == "yes" {
		// start goroutines to delete blobs as they come.
		for i := 0; i < *blobGarbageCollectParallel; i++ {
			eg.Go(func() error {
				for bm := range unused {
					if err := rep.Blobs.DeleteBlob(ctx, bm.BlobID); err != nil {
						return errors.Wrapf(err, "unable to delete blob %q", bm.BlobID)
					}
					cnt, del := deleted.Add(bm.Length)
					if cnt%100 == 0 {
						printStderr("  deleted %v unreferenced blobs (%v)\n", cnt, units.BytesStringBase10(del))
					}
				}

				return nil
			})
		}
	}

	// iterate unreferenced blobs and count them + optionally send to the channel to be deleted
	printStderr("Looking for unreferenced blobs...\n")

	if err := rep.Content.IterateUnreferencedBlobs(ctx, *blobGarbageCollectParallel, func(bm blob.Metadata) error {
		if age := time.Since(bm.Timestamp); age < *blobGarbageCollectMinAge {
			printStderr("  preserving %v because it's too new (age: %v)\n", bm.BlobID, age)
			return nil
		}

		unreferenced.Add(bm.Length)

		if *blobGarbageCollectCommandDelete == "yes" {
			unused <- bm
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "error looking for unreferenced blobs")
	}

	close(unused)

	unreferencedCount, unreferencedSize := unreferenced.Approximate()
	printStderr("Found %v blobs to delete (%v)\n", unreferencedCount, units.BytesStringBase10(unreferencedSize))

	// wait for all delete workers to finish.
	if err := eg.Wait(); err != nil {
		return err
	}

	if *blobGarbageCollectCommandDelete != "yes" {
		if unreferencedCount > 0 {
			printStderr("Pass --delete=yes to delete.\n")
		}

		return nil
	}

	del, cnt := deleted.Approximate()

	printStderr("Deleted total %v unreferenced blobs (%v)\n", del, units.BytesStringBase10(cnt))

	return nil
}

func init() {
	blobGarbageCollectCommand.Action(repositoryAction(runBlobGarbageCollectCommand))
}
