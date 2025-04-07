// Package atomicfile provides wrappers for atomically writing files in a manner compatible with long filenames.
package atomicfile

import (
	"io"
	"runtime"
	"strings"

	"github.com/natefinch/atomic"

	"github.com/kopia/kopia/internal/ospath"
)

// Do not prefix files shorter than this, we are intentionally using less than MAX_PATH
// in Windows to allow some suffixes.
const maxPathLength = 240

// MaybePrefixLongFilenameOnWindows prefixes the given filename with \\?\ on Windows
// if the filename is longer than 260 characters, which is required to be able to
// use some low-level Windows APIs.
// Because long file names have certain limitations:
// - we must replace forward slashes with backslashes.
// - dummy path element (\.\) must be removed.
//
// Relative paths are always limited to a total of MAX_PATH characters:
// https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation
func MaybePrefixLongFilenameOnWindows(fname string) string {
	if runtime.GOOS != "windows" || len(fname) < maxPathLength ||
		fname[:4] == `\\?\` || !ospath.IsAbs(fname) {
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

// Write is a wrapper around atomic.WriteFile that handles long file names on Windows.
func Write(filename string, r io.Reader) error {
	//nolint:wrapcheck
	return atomic.WriteFile(MaybePrefixLongFilenameOnWindows(filename), r)
}
