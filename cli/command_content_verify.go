package cli

import (
	"context"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	contentVerifyCommand = contentCommands.Command("verify", "Verify contents")

	contentVerifyIDs      = contentVerifyCommand.Arg("id", "IDs of blocks to show (or 'all')").Required().Strings()
	contentVerifyParallel = contentVerifyCommand.Flag("parallel", "Parallelism").Int()
)

func runContentVerifyCommand(ctx context.Context, rep *repo.Repository) error {
	for _, contentID := range toContentIDs(*contentVerifyIDs) {
		if contentID == "all" {
			return verifyAllContents(ctx, rep)
		}

		if err := contentVerify(ctx, rep, contentID); err != nil {
			return err
		}
	}

	return nil
}

func verifyAllContents(ctx context.Context, rep *repo.Repository) error {
	var errorCount int32

	err := rep.Content.IterateContents(ctx, content.IterateOptions{
		Parallel: *contentVerifyParallel,
	}, func(ci content.Info) error {
		if err := contentVerify(ctx, rep, ci.ID); err != nil {
			atomic.AddInt32(&errorCount, 1)
		}
		return nil
	})

	if err != nil {
		return errors.Wrap(err, "iterate contents")
	}

	if errorCount == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", errorCount)
}

func contentVerify(ctx context.Context, r *repo.Repository, contentID content.ID) error {
	if _, err := r.Content.GetContent(ctx, contentID); err != nil {
		log(ctx).Warningf("content %v is invalid: %v", contentID, err)
		return err
	}

	log(ctx).Infof("content %v is ok", contentID)

	return nil
}

func init() {
	contentVerifyCommand.Action(repositoryAction(runContentVerifyCommand))
}
