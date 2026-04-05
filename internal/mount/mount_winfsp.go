//go:build windows && winfsp

package mount

import (
	"context"

	cgofuse "github.com/winfsp/cgofuse/fuse"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/winfsp"
)

// Directory mounts a given directory using WinFsp (FUSE) on Windows,
// falling back to WebDAV+net use when PreferWebDAV is set.
func Directory(ctx context.Context, entry fs.Directory, mountPoint string, mountOptions Options) (Controller, error) {
	if mountOptions.PreferWebDAV {
		return directoryWebDAVNetUse(ctx, entry, mountPoint)
	}

	kopiaFS := winfsp.NewKopiaFS(entry)
	host := cgofuse.NewFileSystemHost(kopiaFS)

	// WinFsp mount options for read-only filesystem.
	opts := []string{"-o", "ro", "-o", "uid=-1,gid=-1"}

	done := make(chan struct{})

	go func() {
		host.Mount(mountPoint, opts)
		close(done)
	}()

	return winfspController{
		mountPoint: mountPoint,
		host:       host,
		done:       done,
	}, nil
}

type winfspController struct {
	mountPoint string
	host       *cgofuse.FileSystemHost
	done       chan struct{}
}

func (c winfspController) Unmount(_ context.Context) error {
	c.host.Unmount()
	return nil
}

func (c winfspController) MountPath() string {
	return c.mountPoint
}

func (c winfspController) Done() <-chan struct{} {
	return c.done
}
