package repo

import (
	"context"
	"os"
	"os/user"
	"runtime"
	"strings"
)

// GetDefaultUserName returns default username.
func GetDefaultUserName(ctx context.Context) string {
	currentUser, err := user.Current()
	if err != nil {
		log(ctx).Errorf("Cannot determine current user: %s", err)
		return "nobody"
	}

	u := currentUser.Username

	if runtime.GOOS == "windows" {
		if p := strings.Index(u, "\\"); p >= 0 {
			// On Windows ignore domain name.
			u = u[p+1:]
		}
	}

	return u
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
