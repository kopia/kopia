// +build !windows,!linux,!darwin

package cli

import (
	"github.com/alecthomas/kingpin"
)

func (c *TheApp) setupOSSpecificKeychainFlags(app *kingpin.Application) {
}
