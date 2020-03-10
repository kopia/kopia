package snapshotfs

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// Restore walks a snapshot root with given snapshot ID and restores it to the local filesystem
func Restore(ctx context.Context, rep repo.Repository, targetPath string, snapID manifest.ID, opts localfs.CopyOptions) error {
	m, err := snapshot.LoadSnapshot(ctx, rep, snapID)
	if err != nil {
		return err
	}

	if m.RootEntry == nil {
		return errors.Errorf("No root entry found in manifest (%v)", snapID)
	}

	rootEntry, err := SnapshotRoot(rep, m)
	if err != nil {
		return err
	}

	return localfs.Copy(ctx, targetPath, rootEntry, opts)
}

// RestoreRoot walks a snapshot root with given object ID and restores it to the local filesystem
func RestoreRoot(ctx context.Context, rep repo.Repository, targetPath string, oid object.ID, opts localfs.CopyOptions) error {
	return localfs.Copy(ctx, targetPath, DirectoryEntry(rep, oid, nil), opts)
}
