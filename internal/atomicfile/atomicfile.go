// Package atomicfile provides wrappers for atomically writing files in a manner compatible with long filenames.
package atomicfile

import (
	"io"

	"github.com/natefinch/atomic"

	"github.com/kopia/kopia/internal/ospath"
)

// Write is a wrapper around atomic.WriteFile that handles long file names on Windows.
func Write(filename string, r io.Reader) error {
	//nolint:wrapcheck
	return atomic.WriteFile(ospath.SafeLongFilename(filename), r)
}
