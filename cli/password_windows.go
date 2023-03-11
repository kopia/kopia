package cli

import (
	"github.com/alecthomas/kingpin/v2"
)

func (c *App) setupOSSpecificKeychainFlags(svc appServices, app *kingpin.Application) {
	app.Flag("use-credential-manager", "Use Windows Credential Manager for storing repository password.").Default("true").BoolVar(&c.keyRingEnabled)
}
