// Package ospath provides discovery of OS-dependent paths.
package ospath

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

//nolint:gochecknoglobals
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

// IsAbs determines if a given path is absolute, in particular treating \\hostname\share as absolute on Windows.
func IsAbs(s string) bool {
	// On Windows, treat \\hostname\share as absolute paths, which is not what filepath.IsAbs() does.
	if runtime.GOOS == "windows" {
		if strings.HasPrefix(s, "\\\\") {
			parts := strings.Split(s[2:], "\\")

			return len(parts) > 1 && parts[1] != ""
		}
	}

	//nolint:forbidigo
	if filepath.IsAbs(s) {
		return true
	}

	return false
}

// ResolveUserFriendlyPath replaces ~ in a path with a home directory.
func ResolveUserFriendlyPath(path string, relativeToHome bool) string {
	home, _ := os.UserHomeDir()
	if home != "" && strings.HasPrefix(path, "~") {
		return home + path[1:]
	}

	if IsAbs(path) {
		return path
	}

	if relativeToHome {
		return filepath.Join(home, path)
	}

	return path
}
