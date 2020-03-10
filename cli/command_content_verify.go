package cli

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

var (
	contentVerifyCommand = contentCommands.Command("verify", "Verify that each content is backed by a valid blob")

	contentVerifyIDs      = contentVerifyCommand.Arg("id", "IDs of blocks to show (or 'all')").Required().Strings()
	contentVerifyParallel = contentVerifyCommand.Flag("parallel", "Parallelism").Default("16").Int()
	contentVerifyFull     = contentVerifyCommand.Flag("full", "Full verification (including download)").Bool()
)

func runContentVerifyCommand(ctx context.Context, rep *repo.DirectRepository) error {
	blobMap := map[blob.ID]blob.Metadata{}

	if !*contentVerifyFull {
		printStderr("Listing blobs...\n")

		if err := rep.Blobs.ListBlobs(ctx, "", func(bm blob.Metadata) error {
			blobMap[bm.BlobID] = bm
			if len(blobMap)%10000 == 0 {
				printStderr("  %v blobs...\n", len(blobMap))
			}
			return nil
		}); err != nil {
			return errors.Wrap(err, "unable to list blobs")
		}

		printStderr("Listed %v blobs.\n", len(blobMap))
	}

	for _, contentID := range toContentIDs(*contentVerifyIDs) {
		if contentID == "all" {
			return verifyAllContents(ctx, rep, blobMap)
		}

		ci, err := rep.Content.ContentInfo(ctx, contentID)
		if err != nil {
			return errors.Wrapf(err, "unable to get content info: %v", contentID)
		}

		if err := contentVerify(ctx, rep, &ci, blobMap); err != nil {
			return err
		}
	}

	return nil
}

func verifyAllContents(ctx context.Context, rep *repo.DirectRepository, blobMap map[blob.ID]blob.Metadata) error {
	var totalCount, successCount, errorCount int32

	printStderr("Verifying all contents...\n")

	err := rep.Content.IterateContents(ctx, content.IterateOptions{
		Parallel: *contentVerifyParallel,
	}, func(ci content.Info) error {
		if err := contentVerify(ctx, rep, &ci, blobMap); err != nil {
			log(ctx).Errorf("error %v", err)
			atomic.AddInt32(&errorCount, 1)
		} else {
			atomic.AddInt32(&successCount, 1)
		}

		if t := atomic.AddInt32(&totalCount, 1); t%100000 == 0 {
			printStderr("  %v contents, %v errors...\n", t, atomic.LoadInt32(&errorCount))
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "iterate contents")
	}

	printStderr("Finished verifying %v contents, found %v errors.\n", totalCount, errorCount)

	if errorCount == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", errorCount)
}

func contentVerify(ctx context.Context, r *repo.DirectRepository, ci *content.Info, blobMap map[blob.ID]blob.Metadata) error {
	if *contentVerifyFull {
		if _, err := r.Content.GetContent(ctx, ci.ID); err != nil {
			return errors.Wrapf(err, "content %v is invalid", ci.ID)
		}

		return nil
	}

	bi, ok := blobMap[ci.PackBlobID]
	if !ok {
		return errors.Errorf("content %v depends on missing blob %v", ci.ID, ci.PackBlobID)
	}

	if int64(ci.PackOffset+ci.Length) > bi.Length {
		return errors.Errorf("content %v out of bounds of its pack blob %v", ci.ID, ci.PackBlobID)
	}

	return nil
}

func init() {
	contentVerifyCommand.Action(directRepositoryAction(runContentVerifyCommand))
}
