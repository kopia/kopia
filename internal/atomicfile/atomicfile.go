// Package atomicfile provides wrappers for atomically writing files in a manner compatible with long filenames.
package atomicfile

import (
	"io"
	"runtime"

	"github.com/natefinch/atomic"
)

const maxPathLength = 260

// MaybePrefixLongFilenameOnWindows prefixes the given filename with \\?\ on Windows
// if the filename is longer than 260 characters, which is required to be able to
// use some low-level Windows APIs.
func MaybePrefixLongFilenameOnWindows(fname string) string {
	if runtime.GOOS != "windows" {
		return fname
	}

	if len(fname) < maxPathLength {
		return fname
	}

	return "\\\\?\\" + fname
}

// Write is a wrapper around atomic.WriteFile that handles long file names on Windows.
func Write(filename string, r io.Reader) error {
	return atomic.WriteFile(MaybePrefixLongFilenameOnWindows(filename), r)
}
