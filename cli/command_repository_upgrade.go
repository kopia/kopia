package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var upgradeCommand = repositoryCommands.Command("upgrade", "Upgrade repository format.")

func runUpgradeCommand(ctx context.Context, rep *repo.DirectRepository) error {
	return rep.Upgrade(ctx)
}

func init() {
	upgradeCommand.Action(directRepositoryAction(runUpgradeCommand))
}
