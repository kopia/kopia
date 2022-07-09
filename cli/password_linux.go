package cli

import (
	"github.com/alecthomas/kingpin"
)

func (c *App) setupOSSpecificKeychainFlags(svc appServices, app *kingpin.Application) {
	app.Flag("use-keyring", "Use Gnome Keyring for storing repository password.").Default("false").Envar(svc.EnvName("KOPIA_USE_KEYRING")).BoolVar(&c.keyRingEnabled)
}
