package cli

import (
	"github.com/alecthomas/kingpin"
)

func (c *App) setupOSSpecificKeychainFlags(app *kingpin.Application) {
	app.Flag("use-keyring", "Use Gnome Keyring for storing repository password.").Default("false").BoolVar(&c.keyRingEnabled)
}
