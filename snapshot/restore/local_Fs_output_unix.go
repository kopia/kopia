// +build linux darwin

package restore

import (
	"context"
	"time"

	"golang.org/x/sys/unix"
)

func symlinkChtimes(ctx context.Context, linkPath string, atime, mtime time.Time) error {
	return unix.Lutimes(linkPath, []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	})
}
