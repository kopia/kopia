//go:build windows && winfsp && cgo

package mount

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
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

	if !isValidWindowsDriveOrAsterisk(mountPoint) {
		return nil, errors.Errorf("invalid mount point %q: expected a Windows drive (e.g. \"X:\") or \"*\" for auto-assignment", mountPoint)
	}

	if mountPoint == "*" {
		drive, err := findFreeDriveLetter()
		if err != nil {
			return nil, err
		}

		mountPoint = drive
	}

	kopiaFS := winfsp.NewKopiaFS(entry)
	host := cgofuse.NewFileSystemHost(kopiaFS)

	// WinFsp mount options for read-only local filesystem.
	// FileSystemName=NTFS makes Windows treat it as a trusted local volume.
	opts := []string{"-o", "ro", "-o", "uid=-1,gid=-1", "-o", "FileSystemName=NTFS", "-o", "volname=Kopia"}

	mountErr := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		ok := host.Mount(mountPoint, opts)
		if !ok {
			mountErr <- errors.Errorf("WinFsp mount failed on %v", mountPoint)
		}

		close(done)
	}()

	// Wait briefly for mount to either fail or start serving.
	select {
	case err := <-mountErr:
		return nil, err
	case <-time.After(2 * time.Second): //nolint:mnd
		// Mount is running.
	}

	return winfspController{
		mountPoint: mountPoint,
		host:       host,
		done:       done,
	}, nil
}

// findFreeDriveLetter finds an available drive letter, searching from Z: downward.
func findFreeDriveLetter() (string, error) {
	for c := 'Z'; c >= 'D'; c-- {
		drive := fmt.Sprintf("%c:", c)
		if _, err := os.Stat(drive + "\\"); os.IsNotExist(err) {
			return drive, nil
		}
	}

	return "", errors.New("no free drive letter available")
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
