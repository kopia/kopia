package virtualfs

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

// inmemorySymlink is a mock in-memory implementation of fs.Symlink.
type inmemorySymlink struct {
	entry
}

var _ fs.Symlink = (*inmemorySymlink)(nil)

func (imsl *inmemorySymlink) Readlink(ctx context.Context) (string, error) {
	return "", errors.New("symlinks not supported")
}
