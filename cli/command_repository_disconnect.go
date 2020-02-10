package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var (
	disconnectCommand = repositoryCommands.Command("disconnect", "Disconnect from a repository.")
)

func init() {
	disconnectCommand.Action(noRepositoryAction(runDisconnectCommand))
}

func runDisconnectCommand(ctx context.Context) error {
	return repo.Disconnect(repositoryConfigFileName())
}
