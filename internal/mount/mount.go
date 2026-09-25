// Package mount manages creating operating system mount points for directory snapshots.
package mount

import (
	"context"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("mount")

// Controller allows controlling mounts.
type Controller interface {
	Unmount(ctx context.Context) error
	MountPath() string
	Done() <-chan struct{}
}

// Options cary set of flags passed to the mount layer.
type Options struct {
	// AllowOther allows other users to access the file system. Supported on FUSE"
	FuseAllowOther bool
	// Allows the mounting over a non-empty directory. The files in it will be shadowed by the freshly created mount.
	// Supported only on Fuse.
	FuseAllowNonEmptyMount bool
	// Use WebDAV even on platforms that support FUSE.
	PreferWebDAV bool
	// Port is the port to use for WebDAV server. If 0, a random port will be used.
	Port int
}
