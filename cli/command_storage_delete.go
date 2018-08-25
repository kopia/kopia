package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"
)

var (
	storageDeleteCommand  = storageCommands.Command("delete", "Show storage blocks").Alias("rm")
	storageDeleteBlockIDs = storageDeleteCommand.Arg("blockIDs", "Block IDs").Required().Strings()
)

func runDeleteStorageBlocks(ctx context.Context, rep *repo.Repository) error {
	for _, b := range *storageDeleteBlockIDs {
		err := rep.Storage.DeleteBlock(ctx, b)
		if err != nil {
			return fmt.Errorf("error deleting %v: %v", b, err)
		}
	}

	return nil
}

func init() {
	storageDeleteCommand.Action(repositoryAction(runDeleteStorageBlocks))
}
