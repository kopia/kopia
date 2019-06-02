package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	contentVerifyCommand = contentCommands.Command("verify", "Verify contents")

	contentVerifyIDs = contentVerifyCommand.Arg("id", "IDs of blocks to show (or 'all')").Required().Strings()
)

func runContentVerifyCommand(ctx context.Context, rep *repo.Repository) error {
	for _, contentID := range toContentIDs(*contentVerifyIDs) {
		if contentID == "all" {
			return verifyAllBlocks(ctx, rep)
		}
		if err := contentVerify(ctx, rep, contentID); err != nil {
			return err
		}
	}

	return nil
}

func verifyAllBlocks(ctx context.Context, rep *repo.Repository) error {
	contentIDs, err := rep.Content.ListContents("")
	if err != nil {
		return errors.Wrap(err, "unable to list contents")
	}

	var errorCount int
	for _, contentID := range contentIDs {
		if err := contentVerify(ctx, rep, contentID); err != nil {
			errorCount++
		}
	}
	if errorCount == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", errorCount)
}

func contentVerify(ctx context.Context, r *repo.Repository, contentID content.ID) error {
	if _, err := r.Content.GetContent(ctx, contentID); err != nil {
		log.Warningf("content %v is invalid: %v", contentID, err)
		return err
	}

	log.Infof("content %v is ok", contentID)
	return nil
}

func init() {
	contentVerifyCommand.Action(repositoryAction(runContentVerifyCommand))
}
