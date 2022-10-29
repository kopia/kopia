//go:build openbsd
// +build openbsd

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
			SizeB: uint64(stat.F_blocks) * uint64(stat.F_bsize), //nolint:unconvert,nolintlint
			FreeB: uint64(stat.F_bavail) * uint64(stat.F_bsize), //nolint:unconvert,nolintlint
		}, nil
	}, fs.Impl.(*fsImpl).isRetriable) //nolint:forcetypeassert
}
