package cli

import (
	"bytes"
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	contentShowCommand = contentCommands.Command("show", "Show contents by ID.").Alias("cat")

	contentShowIDs = contentShowCommand.Arg("id", "IDs of contents to show").Required().Strings()
)

func runContentShowCommand(ctx context.Context, rep *repo.DirectRepository) error {
	for _, contentID := range toContentIDs(*contentShowIDs) {
		if err := contentShow(ctx, rep, contentID); err != nil {
			return err
		}
	}

	return nil
}

func contentShow(ctx context.Context, r *repo.DirectRepository, contentID content.ID) error {
	data, err := r.Content.GetContent(ctx, contentID)
	if err != nil {
		return errors.Wrapf(err, "error getting content %v", contentID)
	}

	return showContent(bytes.NewReader(data))
}

func init() {
	setupShowCommand(contentShowCommand)
	contentShowCommand.Action(directRepositoryAction(runContentShowCommand))
}
