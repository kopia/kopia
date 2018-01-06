package ospath

import "os"

func init() {
	userSettingsDir = os.Getenv("APPDATA")
	userCacheDir = os.Getenv("LOCALAPPDATA")
}
