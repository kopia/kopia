// Package ospath provides discovery of OS-dependent paths.
package ospath

import (
	"path/filepath"
)

var (
	userSettingsDir string
	userCacheDir    string
	userLogsDir     string
)

// ConfigDir returns the directory where configuration data (possibly roaming) needs to be stored.
func ConfigDir() string {
	return filepath.Join(userSettingsDir, "kopia")
}

// CacheDir returns the directory where cache data (machine-local) needs to be stored.
func CacheDir() string {
	return filepath.Join(userCacheDir, "kopia")
}

// LogsDir returns the directory where per-user logs should be written.
func LogsDir() string {
	if userLogsDir == "" {
		return filepath.Join(CacheDir(), "logs")
	}

	return filepath.Join(userLogsDir, "kopia")
}
