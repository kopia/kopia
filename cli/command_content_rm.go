package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var (
	contentRemoveCommand = contentCommands.Command("remove", "Remove content").Alias("rm")

	contentRemoveIDs = contentRemoveCommand.Arg("id", "IDs of content to remove").Required().Strings()
)

func runContentRemoveCommand(ctx context.Context, rep *repo.DirectRepository) error {
	advancedCommand(ctx)

	for _, contentID := range toContentIDs(*contentRemoveIDs) {
		if err := rep.Content.DeleteContent(ctx, contentID); err != nil {
			return errors.Wrapf(err, "error deleting content %v", contentID)
		}
	}

	return nil
}

func init() {
	setupShowCommand(contentRemoveCommand)
	contentRemoveCommand.Action(directRepositoryAction(runContentRemoveCommand))
}
