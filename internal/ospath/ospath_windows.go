package ospath

import (
	"os"
)

func init() {
	userSettingsDir = os.Getenv("APPDATA")
	userLogsDir = os.Getenv("LOCALAPPDATA")
}
