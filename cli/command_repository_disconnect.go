package cli

import (
	"context"

	"github.com/kopia/repo"
)

var (
	disconnectCommand = repositoryCommands.Command("disconnect", "Disconnect from a repository.")
)

func init() {
	disconnectCommand.Action(noRepositoryAction(runDisconnectCommand))
}

func runDisconnectCommand(ctx context.Context) error {
	deletePassword(repositoryConfigFileName(), getUserName())
	return repo.Disconnect(repositoryConfigFileName())
}
