// Package mount manages creating operating system mount points for directory snapshots.
package mount

import (
	"context"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("mount")

// Controller allows controlling mounts.
type Controller interface {
	Unmount(ctx context.Context) error
	MountPath() string
	Done() <-chan struct{}
}
