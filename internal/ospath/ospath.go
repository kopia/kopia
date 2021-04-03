// Package ospath provides discovery of OS-dependent paths.
package ospath

import (
	"os"
	"path/filepath"
	"strings"
)

var (
	userSettingsDir string
	userLogsDir     string
)

// ConfigDir returns the directory where configuration data (possibly roaming) needs to be stored.
func ConfigDir() string {
	return filepath.Join(userSettingsDir, "kopia")
}

// LogsDir returns the directory where per-user logs should be written.
func LogsDir() string {
	return filepath.Join(userLogsDir, "kopia")
}

// ResolveUserFriendlyPath replaces ~ in a path with a home directory.
func ResolveUserFriendlyPath(path string, relativeToHome bool) string {
	home, _ := os.UserHomeDir()
	if home != "" && strings.HasPrefix(path, "~") {
		return home + path[1:]
	}

	if filepath.IsAbs(path) {
		return path
	}

	if relativeToHome {
		return filepath.Join(home, path)
	}

	return path
}
