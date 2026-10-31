//go:build openbsd

package cache

import (
	"syscall"

	"github.com/pkg/errors"
)

// getDiskFreeBytes returns the number of bytes available to unprivileged
// processes on the filesystem that contains dir.
func getDiskFreeBytes(dir string) (uint64, error) {
	var stat syscall.Statfs_t

	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, errors.Wrap(err, "Statfs")
	}

	return uint64(stat.F_bavail) * uint64(stat.F_bsize), nil //nolint:unconvert,nolintlint,gosec
}
