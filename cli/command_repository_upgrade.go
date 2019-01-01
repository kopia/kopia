package cli

import (
	"context"

	"github.com/kopia/repo"
)

var (
	upgradeCommand = repositoryCommands.Command("upgrade", "Upgrade repository format.")
)

func runUpgradeCommand(ctx context.Context, rep *repo.Repository) error {
	return rep.Upgrade(ctx)
}

func init() {
	upgradeCommand.Action(repositoryAction(runUpgradeCommand))
}
