// Package virtualfs implements an in-memory filesystem.
package virtualfs

import (
	"os"
	"strings"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("kopia/internal/virtualfs")

// NewDirectory returns a virtual FS root directory.
func NewDirectory(rootName string) (*Directory, error) {
	if strings.Contains(rootName, "/") {
		return nil, errors.New("Root name cannot contain '/'")
	}

	return &Directory{
		entry: entry{
			name: rootName,
			mode: 0777 | os.ModeDir, // nolint:gomnd
		},
	}, nil
}
