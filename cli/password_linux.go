package cli

import (
	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/repo"
)

func (c *TheApp) setupOSSpecificKeychainFlags(app *kingpin.Application) {
	app.Flag("use-keyring", "Use Gnome Keyring for storing repository password.").Default("false").BoolVar(&repo.KeyRingEnabled)
}
