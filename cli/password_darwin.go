package cli

import (
	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo"
)

func (c *App) setupOSSpecificKeychainFlags(app *kingpin.Application) {
	app.Flag("use-keychain", "Use macOS Keychain for storing repository password.").Default("true").BoolVar(&repo.KeyRingEnabled)
}
