//go:build !windows
// +build !windows

package snapshotfs

import (
	"context"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/pkg/errors"
)

func osSnapshotMode(p *policy.Policy) policy.OSSnapshotMode {
	return p.OSSnapshotPolicy.VolumeShadowCopy.Enable.OrDefault(policy.OSSnapshotNever)
}

func createOSSnapshot(ctx context.Context, root fs.Directory) (newRoot fs.Directory, cleanup func(), err error) {
	_, _ = ctx, root
	return nil, nil, errors.New("not supported on this platform")
}
