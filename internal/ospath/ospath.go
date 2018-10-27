// Package ospath provides discovery of OS-dependent paths.
package ospath

import (
	"path/filepath"
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
