package ospath

import (
	"os"
	"strings"
)

func init() {
	userSettingsDir = os.Getenv("APPDATA")
	userLogsDir = os.Getenv("LOCALAPPDATA")
}

// SafeLongFilename prefixes the given filename with \\?\ on Windows when the
// filename length approximates MAX_PATH characters, which is required to be
// able to use some low-level Windows APIs.
// Because long file names have certain limitations:
// - we must replace forward slashes with backslashes.
// - dummy path element (\.\) must be removed.
//
// Relative paths are always limited to a total of MAX_PATH characters (typically 260):
// https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation
func SafeLongFilename(fname string) string {
	// Do not prefix when the name is shorter than this.
	// Intentionally using less than MAX_PATH in Windows to allow some suffixes.
	const maxPathLength = 240

	if len(fname) < maxPathLength ||
		fname[:4] == `\\?\` || !IsAbs(fname) {
		return fname
	}

	fixed := strings.ReplaceAll(fname, "/", `\`)

	for {
		fixed2 := strings.ReplaceAll(fixed, `\.\`, `\`)
		if fixed2 == fixed {
			break
		}

		fixed = fixed2
	}

	return `\\?\` + fixed
}
