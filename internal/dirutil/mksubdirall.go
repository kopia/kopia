// Package dirutil contains directory manipulation helpers.
package dirutil

import (
	"io/fs"

	"github.com/pkg/errors"
)

// ErrTopLevelDirectoryNotFound is returned by MkSubdir if the top-level directory is not found.
var ErrTopLevelDirectoryNotFound = errors.New("top-level directory not found")

// OSInterface represents operating system functions used by MkSubdirAll.
type OSInterface interface {
	Mkdir(dirname string, perm fs.FileMode) error
	IsExist(err error) bool
	IsNotExist(err error) bool
	IsPathSeparator(s byte) bool
}

// MkSubdirAll is like os.MkdirAll(subDir, perm) but will only create missing subdirectories of topLevelDir (but not topLevelDir itself).
// If a top-level directory is unmounted, we want to avoid recreating it, which is what os.MkdirAll() does.
func MkSubdirAll(osi OSInterface, topLevelDir, subDir string, perm fs.FileMode) error {
	topLevelDir = trimTrailingSeparator(osi, topLevelDir)
	subDir = trimTrailingSeparator(osi, subDir)

	if len(subDir) <= len(topLevelDir) {
		return errors.Wrapf(ErrTopLevelDirectoryNotFound, "can't re-create top-level directory %v", topLevelDir)
	}

	err := osi.Mkdir(subDir, perm)
	if osi.IsNotExist(err) {
		// parent does not exist, recursively create it
		if perr := MkSubdirAll(osi, topLevelDir, getParent(osi, subDir), perm); perr != nil {
			return perr
		}

		// try to create the subdir again
		err = osi.Mkdir(subDir, perm)
	}

	switch {
	case err == nil:
		// created subdir, which means parent exists, all good.
		return nil

	case osi.IsExist(err):
		// got an error indicating the directory exists, all good.
		return nil

	default:
		return errors.Wrap(err, "unexpected error creating directory")
	}
}

func trimTrailingSeparator(osi OSInterface, s string) string {
	p := len(s)

	for p > 0 && osi.IsPathSeparator(s[p-1]) {
		p--
	}

	return s[0:p]
}

func getParent(osi OSInterface, s string) string {
	s = trimTrailingSeparator(osi, s)
	p := len(s)

	for p > 0 && !osi.IsPathSeparator(s[p-1]) {
		p--
	}

	return trimTrailingSeparator(osi, s[0:p])
}
