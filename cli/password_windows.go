package cli

import (
	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo"
)

func (c *TheApp) setupOSSpecificKeychainFlags(app *kingpin.Application) {
	app.Flag("use-credential-manager", "Use Windows Credential Manager for storing repository password.").Default("true").BoolVar(&repo.KeyRingEnabled)
}
