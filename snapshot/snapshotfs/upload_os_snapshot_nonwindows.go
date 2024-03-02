//go:build !windows
// +build !windows

package snapshotfs

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot/policy"
)

func osSnapshotMode(*policy.OSSnapshotPolicy) policy.OSSnapshotMode {
	return policy.OSSnapshotNever
}

func createOSSnapshot(context.Context, fs.Directory, *policy.OSSnapshotPolicy) (newRoot fs.Directory, cleanup func(), err error) {
	return nil, nil, errors.New("not supported on this platform")
}
