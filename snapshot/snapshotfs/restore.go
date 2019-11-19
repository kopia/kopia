package snapshotfs

import (
	"context"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
)

// Restore walks a snapshot root with given object ID and restores it to the local filesystem
func Restore(ctx context.Context, rep *repo.Repository, targetPath string, oid object.ID) error {
	return localfs.Copy(ctx, targetPath, DirectoryEntry(rep, oid, nil))
}
