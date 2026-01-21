//go:build !windows && !freebsd && !openbsd

package mount

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

type posixWedavController struct {
	inner      Controller
	mountPoint string
	isTempDir  bool
}

// Directory mounts the given directory using WebDav on macos.
func newPosixWedavController(ctx context.Context, entry fs.Directory, mountPoint string, isTempDir bool) (Controller, error) {
	c, err := DirectoryWebDAV(ctx, entry)
	if err != nil {
		return nil, err
	}

	if err := mountWebDavHelper(ctx, c.MountPath(), mountPoint); err != nil {
		return nil, err
	}

	return posixWedavController{
		inner:      c,
		mountPoint: mountPoint,
		isTempDir:  isTempDir,
	}, nil
}

func (c posixWedavController) Unmount(ctx context.Context) error {
	if err := unmountWebDavHelper(ctx, c.mountPoint); err != nil {
		return err
	}

	if err := c.inner.Unmount(ctx); err != nil {
		// It's conceivable that at this point we could just continue?
		return errors.Wrap(err, "unable to unmount")
	}

	if c.isTempDir {
		if err := os.Remove(c.mountPoint); err != nil {
			return errors.Wrap(err, "unable to remove temporary mount point")
		}
	}

	return nil
}

func (c posixWedavController) MountPath() string {
	return c.mountPoint
}

func (c posixWedavController) Done() <-chan struct{} {
	return c.inner.Done()
}
