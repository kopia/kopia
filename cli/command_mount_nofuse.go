// +build windows

package cli

import (
	"github.com/kopia/kopia/fs"
	"github.com/pkg/errors"
)

var (
	mountMode = mountCommand.Flag("mode", "Mount mode").Default("WEBDAV").Enum("WEBDAV")
)

func mountDirectoryFUSE(entry fs.Directory, mountPoint string) error {
	return errors.New("FUSE is not supported")
}
