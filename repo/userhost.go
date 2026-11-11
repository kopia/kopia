package repo

import (
	"context"
	"os"
	"os/user"
	"runtime"
	"strings"
)

// GetDefaultUserName returns the default username, lowercased so it can be
// used as the user part of a repository server identity (user@hostname).
func GetDefaultUserName(ctx context.Context) string {
	currentUser, err := user.Current()
	if err != nil {
		log(ctx).Errorf("Cannot determine current user: %s", err)
		return "nobody"
	}

	return normalizeDefaultUserName(currentUser.Username, runtime.GOOS == "windows")
}

// normalizeDefaultUserName lowercases the username and, on Windows, drops a
// leading DOMAIN\ prefix. Repository server users must be lowercase
// "user@hostname" (see user.ValidateUsername), so a mixed-case Windows login
// would otherwise have to be overridden by hand.
func normalizeDefaultUserName(u string, windows bool) string {
	if windows {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return strings.ToLower(u)
}

// GetDefaultHostName returns default hostname.
func GetDefaultHostName(ctx context.Context) string {
	hostname, err := os.Hostname()
	if err != nil {
		log(ctx).Errorf("Unable to determine hostname: %s\n", err)
		return "nohost"
	}

	// Normalize hostname.
	hostname = strings.ToLower(strings.Split(hostname, ".")[0])

	return hostname
}
