package cli

import (
	"github.com/kopia/kopia/repo"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	disconnectCommand = app.Command("disconnect", "Disconnect from a repository.")
)

func init() {
	disconnectCommand.Action(runDisconnectCommand)
}

func runDisconnectCommand(context *kingpin.ParseContext) error {
	return repo.Disconnect(repositoryConfigFileName())
}
