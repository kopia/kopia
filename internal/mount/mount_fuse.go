//go:build !windows && !freebsd && !openbsd
// +build !windows,!freebsd,!openbsd

package mount

import (
	"context"
	"os"
	"time"

	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fusemount"
)

// we're serving read-only filesystem, cache some attributes for 30 seconds.
//
//nolint:gochecknoglobals
var cacheTimeout = 30 * time.Second

func (mo *Options) toFuseMountOptions() *gofusefs.Options {
	o := &gofusefs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: mo.FuseAllowOther,
			Name:       "kopia",
			FsName:     "kopia",
			Debug:      os.Getenv("KOPIA_DEBUG_FUSE") != "",
		},
		EntryTimeout:    &cacheTimeout,
		AttrTimeout:     &cacheTimeout,
		NegativeTimeout: &cacheTimeout,
	}

	o.Options = append(o.Options, "noatime")
	if mo.FuseAllowNonEmptyMount {
		o.Options = append(o.Options, "nonempty")
	}

	return o
}

// Directory mounts the given directory using FUSE.
func Directory(ctx context.Context, entry fs.Directory, mountPoint string, mountOptions Options) (Controller, error) {
	isTempDir := false

	if mountPoint == "*" {
		var err error

		mountPoint, err = os.MkdirTemp("", "kopia-mount")
		if err != nil {
			return nil, errors.Wrap(err, "error creating temp directory")
		}

		isTempDir = true
	}

	if mountOptions.PreferWebDAV {
		return newPosixWedavController(ctx, entry, mountPoint, isTempDir)
	}

	rootNode := fusemount.NewDirectoryNode(entry)

	fuseServer, err := gofusefs.Mount(mountPoint, rootNode, mountOptions.toFuseMountOptions())
	if err != nil {
		return nil, errors.Wrap(err, "mounting error")
	}

	done := make(chan struct{})

	go func() {
		fuseServer.Wait()
		close(done)
	}()

	return fuseController{mountPoint, fuseServer, done, isTempDir}, nil
}

type fuseController struct {
	mountPoint     string
	fuseConnection *fuse.Server
	done           chan struct{}
	isTempDir      bool
}

func (fc fuseController) MountPath() string {
	return fc.mountPoint
}

func (fc fuseController) Unmount(ctx context.Context) error {
	if err := fc.fuseConnection.Unmount(); err != nil {
		return errors.Wrap(err, "unmount error")
	}

	if fc.isTempDir {
		if err := os.Remove(fc.mountPoint); err != nil {
			return errors.Wrap(err, "unable to remove temporary mount point")
		}
	}

	return nil
}

func (fc fuseController) Done() <-chan struct{} {
	return fc.done
}
