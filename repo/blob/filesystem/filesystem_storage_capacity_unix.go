//go:build linux || freebsd || darwin
// +build linux freebsd darwin

package filesystem

import (
	"context"
	"syscall"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/repo/blob"
)

func (fs *fsStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	c, err := retry.WithExponentialBackoff(ctx, "GetCapacity", func() (interface{}, error) {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(fs.RootPath, &stat); err != nil {
			return blob.Capacity{}, errors.Wrap(err, "GetCapacity")
		}

		return blob.Capacity{
			SizeB: uint64(stat.Blocks) * uint64(stat.Bsize), //nolint:unconvert
			FreeB: uint64(stat.Bavail) * uint64(stat.Bsize), //nolint:unconvert
		}, nil
	}, fs.Impl.(*fsImpl).isRetriable)

	return c.(blob.Capacity), err //nolint:forcetypeassert,wrapcheck
}
