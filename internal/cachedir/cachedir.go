// Package cachedir contains utilities for manipulating cache directories.
package cachedir

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// CacheDirMarkerFile is the name of the marker file indicating a directory contains Kopia caches.
// See https://bford.info/cachedir/
const CacheDirMarkerFile = "CACHEDIR.TAG"

// CacheDirMarkerHeader is the header signature for cache dir marker files.
const CacheDirMarkerHeader = "Signature: 8a477f597d28d172789f06886806bc55"

const cacheDirMarkerContents = CacheDirMarkerHeader + `
#
# This file is a cache directory tag created by Kopia - Fast And Secure Open-Source Backup.
#
# For information about Kopia, see:
#   https://kopia.io
#
# For information about cache directory tags, see:
#   http://www.brynosaurus.com/cachedir/
`

// WriteCacheMarker writes the CACHEDIR.TAG marker file in a given directory.
func WriteCacheMarker(cacheDir string) error {
	if cacheDir == "" {
		return nil
	}

	markerFile := filepath.Join(cacheDir, CacheDirMarkerFile)

	st, err := os.Stat(markerFile)
	if err == nil && st.Size() >= int64(len(cacheDirMarkerContents)) {
		// ok
		return nil
	}

	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "unexpected cache marker error")
	}

	f, err := os.Create(markerFile) //nolint:gosec
	if err != nil {
		return errors.Wrap(err, "error creating cache marker")
	}

	if _, err := f.WriteString(cacheDirMarkerContents); err != nil {
		return errors.Wrap(err, "unable to write cachedir marker contents")
	}

	return errors.Wrap(f.Close(), "error closing cache marker file")
}
