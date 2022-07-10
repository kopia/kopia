//go:build !windows && !linux && !darwin
// +build !windows,!linux,!darwin

package cli

import (
	"github.com/alecthomas/kingpin"
)

func (c *App) setupOSSpecificKeychainFlags(svc appServices, app *kingpin.Application) {
}
