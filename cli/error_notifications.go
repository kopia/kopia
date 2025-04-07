package cli

import (
	"os"

	"github.com/mattn/go-isatty"
)

const (
	errorNotificationsNever          = "never"
	errorNotificationsAlways         = "always"
	errorNotificationsNonInteractive = "non-interactive"
)

func (c *App) enableErrorNotifications() bool {
	switch c.errorNotifications {
	case errorNotificationsNever:
		return false

	case errorNotificationsAlways:
		return true

	case errorNotificationsNonInteractive:
		if c.isInProcessTest {
			return false
		}

		if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
			// interactive terminal, don't send notifications
			return false
		}

		return true

	default:
		return false
	}
}
