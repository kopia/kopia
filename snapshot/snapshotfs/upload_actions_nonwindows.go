//go:build !windows
// +build !windows

package snapshotfs

import (
	"context"

	"github.com/kopia/kopia/fs"
	"github.com/pkg/errors"
)

func createShadowCopy(ctx context.Context, root fs.Directory) (newRoot fs.Directory, cleanup func(), err error) {
	_, _ = ctx, root
	return nil, nil, errors.New("not supported on this platform")
}
