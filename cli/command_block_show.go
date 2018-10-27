package cli

import (
	"bytes"
	"context"

	"github.com/kopia/repo"
)

var (
	showBlockCommand = blockCommands.Command("show", "Show contents of a block.").Alias("cat")

	showBlockIDs = showBlockCommand.Arg("id", "IDs of blocks to show").Required().Strings()
)

func runShowBlockCommand(ctx context.Context, rep *repo.Repository) error {
	for _, blockID := range *showBlockIDs {
		if err := showBlock(ctx, rep, blockID); err != nil {
			return err
		}
	}

	return nil
}

func showBlock(ctx context.Context, r *repo.Repository, blockID string) error {
	data, err := r.Blocks.GetBlock(ctx, blockID)
	if err != nil {
		return err
	}

	return showContent(bytes.NewReader(data))
}

func init() {
	setupShowCommand(showBlockCommand)
	showBlockCommand.Action(repositoryAction(runShowBlockCommand))
}
