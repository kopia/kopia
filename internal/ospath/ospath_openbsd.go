package ospath

import (
	"os"
	"path/filepath"
)

func init() {
	userSettingsDir = filepath.Join(os.Getenv("HOME"), ".config")
	userLogsDir = filepath.Join(os.Getenv("HOME"), ".cache")
}
