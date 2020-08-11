// +build windows

package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

var mountMode = mountCommand.Flag("mode", "Mount mode").Default("WEBDAV").Enum("WEBDAV")

func mountDirectoryFUSE(ctx context.Context, entry fs.Directory, mountPoint string) error {
	return errors.New("FUSE is not supported")
}
