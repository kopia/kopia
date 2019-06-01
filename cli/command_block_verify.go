package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var (
	verifyBlockCommand = blockCommands.Command("verify", "Verify contents of a block.")

	verifyBlockIDs = verifyBlockCommand.Arg("id", "IDs of blocks to show (or 'all')").Required().Strings()
)

func runVerifyBlockCommand(ctx context.Context, rep *repo.Repository) error {
	for _, blockID := range *verifyBlockIDs {
		if blockID == "all" {
			return verifyAllBlocks(ctx, rep)
		}
		if err := verifyBlock(ctx, rep, blockID); err != nil {
			return err
		}
	}

	return nil
}

func verifyAllBlocks(ctx context.Context, rep *repo.Repository) error {
	blockIDs, err := rep.Blocks.ListBlocks("")
	if err != nil {
		return errors.Wrap(err, "unable to list blocks")
	}

	var errorCount int
	for _, blockID := range blockIDs {
		if err := verifyBlock(ctx, rep, blockID); err != nil {
			errorCount++
		}
	}
	if errorCount == 0 {
		return nil
	}

	return errors.Errorf("encountered %v errors", errorCount)
}

func verifyBlock(ctx context.Context, r *repo.Repository, blockID string) error {
	if _, err := r.Blocks.GetBlock(ctx, blockID); err != nil {
		log.Warningf("block %v is invalid: %v", blockID, err)
		return err
	}

	log.Infof("block %v is ok", blockID)

	return nil
}

func init() {
	verifyBlockCommand.Action(repositoryAction(runVerifyBlockCommand))
}
