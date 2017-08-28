// +build windows

package cli

import (
	"fmt"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fscache"
)

var (
	mountMode = mountCommand.Flag("mode", "Mount mode").Default("WEBDAV").Enum("WEBDAV")
)

func mountDirectoryFUSE(entry fs.Directory, mountPoint string, cache *fscache.Cache) error {
	return fmt.Errorf("FUSE is not supported")
}
