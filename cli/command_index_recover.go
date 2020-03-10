package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var (
	blockIndexRecoverCommand = indexCommands.Command("recover", "Recover indexes from pack blobs")
	blockIndexRecoverBlobIDs = blockIndexRecoverCommand.Flag("blobs", "Names of pack blobs to recover from (default=all packs)").Strings()
	blockIndexRecoverCommit  = blockIndexRecoverCommand.Flag("commit", "Commit recovered content").Bool()
)

func runRecoverBlockIndexesAction(ctx context.Context, rep *repo.DirectRepository) error {
	var totalCount int

	defer func() {
		if totalCount == 0 {
			log(ctx).Infof("No blocks recovered.")
			return
		}

		if !*blockIndexRecoverCommit {
			log(ctx).Infof("Found %v blocks to recover, but not committed. Re-run with --commit", totalCount)
		} else {
			log(ctx).Infof("Recovered %v blocks.", totalCount)
		}
	}()

	if len(*blockIndexRecoverBlobIDs) == 0 {
		for _, prefix := range content.PackBlobIDPrefixes {
			err := rep.Blobs.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
				recoverIndexFromSinglePackFile(ctx, rep, bm.BlobID, bm.Length, &totalCount)
				return nil
			})
			if err != nil {
				return errors.Wrapf(err, "recovering indexes from prefix %q", prefix)
			}
		}
	}

	for _, packFile := range *blockIndexRecoverBlobIDs {
		recoverIndexFromSinglePackFile(ctx, rep, blob.ID(packFile), 0, &totalCount)
	}

	return nil
}

func recoverIndexFromSinglePackFile(ctx context.Context, rep *repo.DirectRepository, blobID blob.ID, length int64, totalCount *int) {
	recovered, err := rep.Content.RecoverIndexFromPackBlob(ctx, blobID, length, *blockIndexRecoverCommit)
	if err != nil {
		log(ctx).Warningf("unable to recover index from %v: %v", blobID, err)
		return
	}

	*totalCount += len(recovered)
	log(ctx).Infof("Recovered %v entries from %v (commit=%v)", len(recovered), blobID, *blockIndexRecoverCommit)
}

func init() {
	blockIndexRecoverCommand.Action(directRepositoryAction(runRecoverBlockIndexesAction))
}
