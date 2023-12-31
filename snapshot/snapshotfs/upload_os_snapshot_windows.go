package snapshotfs

import (
	"context"
	"path/filepath"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/mxk/go-vss"
	"github.com/pkg/errors"
)

func osSnapshotMode(p *policy.Policy) policy.OSSnapshotMode {
	return p.OSSnapshotPolicy.VolumeShadowCopy.Enable.OrDefault(policy.OSSnapshotWhenAvailable)
}

func createOSSnapshot(ctx context.Context, root fs.Directory) (newRoot fs.Directory, cleanup func(), err error) {
	local := root.LocalFilesystemPath()
	if local == "" {
		return nil, nil, errors.New("not a local filesystem")
	}
	vol, rel, err := vss.SplitVolume(local)
	if err != nil {
		return nil, nil, err
	}

	uploadLog(ctx).Infof("creating volume shadow copy of %v", vol)
	id, err := vss.Create(vol)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			_ = vss.Remove(id)
		}
	}()

	uploadLog(ctx).Infof("new volume shadow copy id %s", id)
	sc, err := vss.Get(id)
	if err != nil {
		return nil, nil, err
	}

	newRoot, err = localfs.Directory(filepath.Join(sc.DeviceObject, rel))
	if err != nil {
		return nil, nil, err
	}
	uploadLog(ctx).Debugf("shadow copy root is %s", newRoot.LocalFilesystemPath())

	cleanup = func() {
		uploadLog(ctx).Infof("removing volume shadow copy id %s", id)
		if err := vss.Remove(id); err != nil {
			uploadLog(ctx).Errorf("failed to remove volume shadow copy: %v", err)
		}
	}
	return
}
