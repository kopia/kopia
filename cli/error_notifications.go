package cli

import (
	"os"

	"golang.org/x/term"
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

		if fd, err := intFd(os.Stdout); err == nil && term.IsTerminal(fd) {
			// interactive terminal, don't send notifications
			return false
		}

		return true

	default:
		return false
	}
}
