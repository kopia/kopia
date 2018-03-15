package ospath

import (
	"os"
	"path/filepath"
)

func init() {
	userSettingsDir = filepath.Join(os.Getenv("HOME"), "Library", "Application Support")
	userCacheDir = filepath.Join(os.Getenv("HOME"), "Library", "Caches")
	userLogsDir = filepath.Join(os.Getenv("HOME"), "Library", "Logs")
}
