// +build windows

package cli

import (
	"fmt"

	"github.com/kopia/kopia/fs"
)

var (
	mountMode = mountCommand.Flag("mode", "Mount mode").Default("WEBDAV").Enum("WEBDAV")
)

func mountDirectoryFUSE(entry fs.Directory, mountPoint string) error {
	return fmt.Errorf("FUSE is not supported")
}
