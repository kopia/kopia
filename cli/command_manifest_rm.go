package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
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
		rep.Manifests.Delete(it)
	}

	return nil
}
