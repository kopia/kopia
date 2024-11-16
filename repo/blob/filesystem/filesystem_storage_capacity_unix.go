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
	return retry.WithExponentialBackoff(ctx, "GetCapacity", func() (blob.Capacity, error) {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(fs.RootPath, &stat); err != nil {
			return blob.Capacity{}, errors.Wrap(err, "GetCapacity")
		}

		return blob.Capacity{
			SizeB: uint64(stat.Blocks) * uint64(stat.Bsize), //nolint:gosec,unconvert,nolintlint
			FreeB: uint64(stat.Bavail) * uint64(stat.Bsize), //nolint:gosec,unconvert,nolintlint
		}, nil
	}, fs.Impl.(*fsImpl).isRetriable) //nolint:forcetypeassert
}
