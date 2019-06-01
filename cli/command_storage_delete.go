package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/pkg/errors"
)

var (
	storageDeleteCommand  = storageCommands.Command("delete", "Show storage blocks").Alias("rm")
	storageDeleteBlockIDs = storageDeleteCommand.Arg("blockIDs", "Block IDs").Required().Strings()
)

func runDeleteStorageBlocks(ctx context.Context, rep *repo.Repository) error {
	for _, b := range *storageDeleteBlockIDs {
		err := rep.Storage.DeleteBlock(ctx, b)
		if err != nil {
			return errors.Wrapf(err, "error deleting %v", b)
		}
	}

	return nil
}

func init() {
	storageDeleteCommand.Action(repositoryAction(runDeleteStorageBlocks))
}
