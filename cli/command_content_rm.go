package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var (
	contentRemoveCommand = contentCommands.Command("remove", "Remove content").Alias("rm")

	contentRemoveIDs = contentRemoveCommand.Arg("id", "IDs of content to remove").Required().Strings()
)

func runContentRemoveCommand(ctx context.Context, rep *repo.DirectRepository) error {
	for _, contentID := range toContentIDs(*contentRemoveIDs) {
		if err := rep.Content.DeleteContent(ctx, contentID); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	setupShowCommand(contentRemoveCommand)
	contentRemoveCommand.Action(directRepositoryAction(runContentRemoveCommand))
}
