package filesystemSnapshots

import (
	"context"
	"path/filepath"

	fs_snapshot "github.com/pescuma/go-fs-snapshot/lib"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs/mappedfs"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot/policy"
)

var log = logging.Module("kopia/filesystemSnapshots")

// NoSnapshots is a noop mapping
type NoSnapshots struct {
}

// Apply is noop.
func (n NoSnapshots) Apply(path string) (string, error) {
	return path, nil
}

// Close is noop.
func (n NoSnapshots) Close() {
}

// FsSnapshot creates a filesystem mapping basend on fs_snapshot and configured by policies.
func FsSnapshot(ctx context.Context, path string, policyTree *policy.Tree) mappedfs.FilesystemMapper {
	return &fsSnapshot{
		ctx:        ctx,
		path:       path,
		policyTree: policyTree,
	}
}

type fsSnapshot struct {
	ctx         context.Context
	policyTree  *policy.Tree
	path        string
	initialized bool
	snapshoter  fs_snapshot.Snapshoter
	backuper    fs_snapshot.Backuper
}

// Apply creates a filesystem snapshot if configured in the policies.
func (s *fsSnapshot) Apply(originalPath string) (string, error) {
	rel, err := filepath.Rel(s.path, originalPath)
	if err != nil {
		//nolint:wrapcheck
		return "", err
	}

	policyTree := s.policyTree.Child(rel)

	// TODO add option to require snapshots
	if !policyTree.EffectivePolicy().FilesPolicy.UseFsSnapshots.OrDefault(false) {
		return originalPath, nil
	}

	s.init()

	if s.snapshoter == nil {
		return originalPath, nil
	}

	snapshotPath, err := s.backuper.TryToCreateTemporarySnapshot(originalPath)
	if err != nil {
		if !errors.Is(err, fs_snapshot.ErrorSnapshotFailedInPreviousAttempt) {
			log(s.ctx).Errorf("Error creating filesystem snapshot for '%v': %v. Ignoring and using original files.", originalPath, err)
		}

		return originalPath, nil
	}

	return snapshotPath, nil
}

func (s *fsSnapshot) init() {
	if s.initialized {
		return
	}

	s.initialized = true

	snapshoter, err := fs_snapshot.NewSnapshoter(nil)
	if err != nil {
		log(s.ctx).Errorf("Error creating filesystem snapshot: %v\n", err)
		return
	}

	backuper, err := snapshoter.StartBackup(nil)
	if err != nil {
		log(s.ctx).Errorf("Error creating filesystem snapshot: %v\n", err)

		snapshoter.Close()

		return
	}

	s.backuper = backuper
	s.snapshoter = snapshoter
}

// Close frees resources.
func (s *fsSnapshot) Close() {
	if s.backuper != nil {
		s.backuper.Close()
		s.backuper = nil
	}

	if s.snapshoter != nil {
		s.snapshoter.Close()
		s.snapshoter = nil
	}
}

var (
	_ mappedfs.FilesystemMapper = &fsSnapshot{}
	_ mappedfs.FilesystemMapper = &NoSnapshots{}
)
