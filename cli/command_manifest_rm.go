package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var (
	manifestRemoveCommand = manifestCommands.Command("rm", "Remove manifest items")
	manifestRemoveItems   = manifestRemoveCommand.Arg("item", "Items to remove").Required().Strings()
)

func runManifestRemoveCommand(ctx context.Context, rep repo.Repository) error {
	for _, it := range toManifestIDs(*manifestRemoveItems) {
		if err := rep.DeleteManifest(ctx, it); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	manifestRemoveCommand.Action(repositoryAction(runManifestRemoveCommand))
}
