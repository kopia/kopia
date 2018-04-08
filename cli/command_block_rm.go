package cli

import (
	"context"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/repo"
)

var (
	removeBlockCommand = blockCommands.Command("remove", "Remove block(s)").Alias("rm")

	removeBlockIDs = removeBlockCommand.Arg("id", "IDs of blocks to remove").Required().Strings()
)

func runRemoveBlockCommand(ctx context.Context, rep *repo.Repository) error {
	for _, blockID := range *removeBlockIDs {
		if err := removeBlock(rep, block.ContentID(blockID)); err != nil {
			return err
		}
	}

	return nil
}

func removeBlock(r *repo.Repository, blockID block.ContentID) error {
	return r.Blocks.DeleteBlock(blockID)
}

func init() {
	setupShowCommand(removeBlockCommand)
	removeBlockCommand.Action(repositoryAction(runRemoveBlockCommand))
}
