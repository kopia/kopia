//go:build !windows && !linux && !darwin
// +build !windows,!linux,!darwin

package cli

import (
	"github.com/alecthomas/kingpin/v2"
)

//nolint:revive
func (c *App) setupOSSpecificKeychainFlags(svc appServices, app *kingpin.Application) {
}
