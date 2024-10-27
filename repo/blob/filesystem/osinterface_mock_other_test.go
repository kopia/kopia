//go:build !linux && !freebsd && !darwin
// +build !linux,!freebsd,!darwin

package filesystem

import (
	"io/fs"
	"os"

	"github.com/pkg/errors"
)

func (osi *mockOS) Stat(fname string) (fs.FileInfo, error) {
	if osi.statRemainingErrors.Add(-1) >= 0 {
		return nil, &os.PathError{Op: "stat", Err: errors.New("underlying problem")}
	}

	return osi.osInterface.Stat(fname)
}
