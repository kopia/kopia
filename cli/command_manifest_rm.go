package cli

import (
	"context"

	"github.com/kopia/repo"
)

var (
	manifestRemoveCommand = manifestCommands.Command("rm", "Remove manifest items")
	manifestRemoveItems   = manifestRemoveCommand.Arg("item", "Items to remove").Required().Strings()
)

func init() {
	manifestRemoveCommand.Action(repositoryAction(removeMetadataItem))
}

func removeMetadataItem(ctx context.Context, rep *repo.Repository) error {
	for _, it := range *manifestRemoveItems {
		if err := rep.Manifests.Delete(ctx, it); err != nil {
			return err
		}
	}

	return nil
}
