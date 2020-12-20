package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/maintenance"
)

var (
	blobGarbageCollectCommand       = blobCommands.Command("gc", "Garbage-collect unused blobs")
	blobGarbageCollectCommandDelete = blobGarbageCollectCommand.Flag("delete", "Whether to delete unused blobs").String()
	blobGarbageCollectParallel      = blobGarbageCollectCommand.Flag("parallel", "Number of parallel blob scans").Default("16").Int()
	blobGarbageCollectMinAge        = blobGarbageCollectCommand.Flag("min-age", "Garbage-collect blobs with minimum age").Default("24h").Duration()
	blobGarbageCollectPrefix        = blobGarbageCollectCommand.Flag("prefix", "Only GC blobs with given prefix").String()
)

func runBlobGarbageCollectCommand(ctx context.Context, rep *repo.DirectRepository) error {
	advancedCommand(ctx)

	opts := maintenance.DeleteUnreferencedBlobsOptions{
		DryRun:   *blobGarbageCollectCommandDelete != "yes",
		MinAge:   *blobGarbageCollectMinAge,
		Parallel: *blobGarbageCollectParallel,
		Prefix:   blob.ID(*blobGarbageCollectPrefix),
	}

	n, err := maintenance.DeleteUnreferencedBlobs(ctx, rep, opts)
	if err != nil {
		return errors.Wrap(err, "error deleting unreferenced blobs")
	}

	if opts.DryRun && n > 0 {
		log(ctx).Infof("Pass --delete=yes to delete.")
	}

	return nil
}

func init() {
	blobGarbageCollectCommand.Action(directRepositoryAction(runBlobGarbageCollectCommand))
}
