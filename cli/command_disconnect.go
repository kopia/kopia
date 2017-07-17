package cli

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	disconnectCommand = app.Command("disconnect", "Connect to a vault.")
)

func init() {
	disconnectCommand.Action(runDisconnectCommand)
}

func runDisconnectCommand(context *kingpin.ParseContext) error {
	fn := vaultConfigFileName()
	if _, err := os.Stat(fn); err == nil {
		return os.Remove(fn)
	}
	return nil
}
