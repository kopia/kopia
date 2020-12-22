// +build !windows

package mount

import (
	"context"
	"io/ioutil"
	"os"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fusemount"
)

type root struct {
	fusefs.Node
}

func (r *root) Root() (fusefs.Node, error) {
	return r.Node, nil
}

func (mo *Options) toFuseMountOptions() []fuse.MountOption {
	options := []fuse.MountOption{
		fuse.ReadOnly(),
		fuse.FSName("kopia"),
		fuse.Subtype("kopia"),
		fuse.VolumeName("Kopia"),
	}

	if mo.FuseAllowOther {
		options = append(options, fuse.AllowOther())
	}

	if mo.FuseAllowNonEmptyMount {
		options = append(options, fuse.AllowNonEmptyMount())
	}

	return options
}

// Directory mounts the given directory using FUSE.
func Directory(ctx context.Context, entry fs.Directory, mountPoint string, mountOptions Options) (Controller, error) {
	isTempDir := false

	if mountPoint == "*" {
		var err error

		mountPoint, err = ioutil.TempDir("", "kopia-mount")
		if err != nil {
			return nil, errors.Wrap(err, "error creating temp directory")
		}

		isTempDir = true
	}

	rootNode := fusemount.NewDirectoryNode(entry)

	options := append(
		mountOptions.toFuseMountOptions(),
		fuse.ReadOnly(),
		fuse.FSName("kopia"),
		fuse.Subtype("kopia"),
		fuse.VolumeName("Kopia"))

	fuseConnection, err := fuse.Mount(mountPoint, options...)
	if err != nil {
		return nil, errors.Wrap(err, "error creating fuse connection")
	}

	serveError := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		serveError <- fusefs.Serve(fuseConnection, &root{rootNode})

		fuseConnection.Close() // nolint:errcheck
		close(done)
	}()

	select {
	case err := <-serveError:
		log(ctx).Debugf("serve error: %v", err)
		return nil, errors.Wrap(err, "serve error")

	case <-fuseConnection.Ready:
		log(ctx).Debugf("connection ready: %v", fuseConnection.MountError)

		if err := fuseConnection.MountError; err != nil {
			return nil, errors.Wrap(err, "mount error")
		}
	}

	return fuseController{mountPoint, fuseConnection, done, isTempDir}, nil
}

type fuseController struct {
	mountPoint     string
	fuseConnection *fuse.Conn
	done           chan struct{}
	isTempDir      bool
}

func (fc fuseController) MountPath() string {
	return fc.mountPoint
}

func (fc fuseController) Unmount(ctx context.Context) error {
	if err := fuse.Unmount(fc.mountPoint); err != nil {
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
