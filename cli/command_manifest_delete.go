package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

var (
	manifestRemoveCommand = manifestCommands.Command("delete", "Remove manifest items").Alias("remove").Alias("rm")
	manifestRemoveItems   = manifestRemoveCommand.Arg("item", "Items to remove").Required().Strings()
)

func runManifestRemoveCommand(ctx context.Context, rep repo.RepositoryWriter) error {
	advancedCommand(ctx)

	for _, it := range toManifestIDs(*manifestRemoveItems) {
		if err := rep.DeleteManifest(ctx, it); err != nil {
			return errors.Wrapf(err, "unable to delete manifest %v", it)
		}
	}

	return nil
}

func init() {
	manifestRemoveCommand.Action(repositoryWriterAction(runManifestRemoveCommand))
}
