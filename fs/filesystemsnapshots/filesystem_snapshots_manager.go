package filesystemsnapshots

import (
	"context"
	"sync"

	"github.com/pescuma/go-fs-snapshot/lib/fs_snapshot"

	"github.com/kopia/kopia/fs/mappedfs"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var log = logging.Module("kopia/filesystemsnapshots")

// FilesystemSnapshotManager allows to use the same snapshots for multiple paths.
// This helps because some OSes does not allow to map more than one filesystem snapshot at the same time,
// and in windows it takes a lot of CPU to create one.
type FilesystemSnapshotManager interface {
	CreateMapper(ctx context.Context, root string, policyTree *policy.Tree, manifest *snapshot.Manifest) mappedfs.FilesystemMapper
	Close()
}

// CreateFsSnapshotsManager creates a FilesystemSnapshotManager based on fs_snapshot.
func CreateFsSnapshotsManager() FilesystemSnapshotManager {
	return &fsSnapshotsManager{}
}

type fsSnapshotsManager struct {
	mutex       sync.RWMutex
	initialized bool
	initError   error
	snapshoter  fs_snapshot.Snapshoter
	backuper    fs_snapshot.Backuper
}

// CreateMapper creates a filesystem mapping based on fs_snapshot and configured by policies.
func (f *fsSnapshotsManager) CreateMapper(ctx context.Context, root string, policyTree *policy.Tree, manifest *snapshot.Manifest) mappedfs.FilesystemMapper {
	return &fsSnapshotsMapper{
		getOrCreateBackuper: f.getOrCreateBackuper,
		log:                 log(ctx),
		root:                root,
		policyTree:          policyTree,
		manifest:            manifest,
		snapshotIDs:         map[string]bool{},
	}
}

func (f *fsSnapshotsManager) getOrCreateBackuper() (fs_snapshot.Backuper, error) {
	f.mutex.RLock()
	wasInitialized := f.initialized
	f.mutex.RUnlock()

	if wasInitialized {
		return f.backuper, f.initError
	}

	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.initialized {
		return f.backuper, f.initError
	}

	f.initialized = true

	snapshoter, err := fs_snapshot.NewSnapshoter(nil)
	if err != nil {
		f.initError = err

		return nil, f.initError
	}

	backuper, err := snapshoter.StartBackup(nil)
	if err != nil {
		f.initError = err

		snapshoter.Close()

		return nil, f.initError
	}

	f.backuper = backuper
	f.snapshoter = snapshoter

	return f.backuper, f.initError
}

func (f *fsSnapshotsManager) Close() {
	if f.backuper != nil {
		f.backuper.Close()
		f.backuper = nil
	}

	if f.snapshoter != nil {
		f.snapshoter.Close()
		f.snapshoter = nil
	}
}

var _ FilesystemSnapshotManager = &fsSnapshotsManager{}
