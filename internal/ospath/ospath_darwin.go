package ospath

import (
	"os"
	"path/filepath"
)

func init() {
	userSettingsDir = filepath.Join(os.Getenv("HOME"), "Library", "Application Support")
	userLogsDir = filepath.Join(os.Getenv("HOME"), "Library", "Logs")
}
