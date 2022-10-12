// Package filesystemsnapshots implements support for filesystem snapshots based on mappedfs.
package filesystemsnapshots

import (
	"context"
	"path/filepath"

	"github.com/pkg/errors"

	fs_snapshot "github.com/pescuma/go-fs-snapshot/lib"

	"github.com/kopia/kopia/fs/mappedfs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

var log = logging.Module("kopia/filesystemsnapshots")

// NoSnapshots is a noop mapping.
type NoSnapshots struct{}

// Apply is noop.
func (n NoSnapshots) Apply(path string) (string, error) {
	return path, nil
}

// Close is noop.
func (n NoSnapshots) Close() {
}

// FsSnapshot creates a filesystem mapping based on fs_snapshot and configured by policies.
func FsSnapshot(ctx context.Context, root string, policyTree *policy.Tree, manifest *snapshot.Manifest) mappedfs.FilesystemMapper {
	return &fsSnapshot{
		log:         log(ctx),
		root:        root,
		policyTree:  policyTree,
		manifest:    manifest,
		snapshotIDs: map[string]bool{},
	}
}

type fsSnapshot struct {
	log         logging.Logger
	policyTree  *policy.Tree
	manifest    *snapshot.Manifest
	root        string
	initialized bool
	snapshoter  fs_snapshot.Snapshoter
	backuper    fs_snapshot.Backuper
	snapshotIDs map[string]bool
}

// Apply creates a filesystem snapshot if configured in the policies.
func (s *fsSnapshot) Apply(originalDir string) (string, error) {
	rel, err := filepath.Rel(s.root, originalDir)
	if err != nil {
		return "", errors.Wrapf(err, "%v should be inside %v", originalDir, s.root)
	}

	policyTree := s.policyTree.Child(rel)

	var required bool

	switch policyTree.EffectivePolicy().FilesPolicy.UseFilesystemSnapshots {
	case "required":
		required = true
	case "if available":
		required = false
	default:
		return originalDir, nil
	}

	if !s.initialized {
		s.initialized = true

		err = s.init()
		if err != nil {
			if required {
				return "", errors.Wrapf(err, "error creating filesystem snapshot engine")
			}

			s.manifest.FilesystemSnapshots = append(s.manifest.FilesystemSnapshots,
				snapshot.FilesystemSnapshotInfo{
					Path:      originalDir,
					Timestamp: clock.Now(),
					Error:     err.Error(),
				})

			s.log.Errorf("Error creating filesystem snapshot engine: %v. Ignoring and using original files.", err)
		}
	}

	if s.snapshoter == nil {
		return originalDir, nil
	}

	snapshotDir, snapshotInfo, err := s.backuper.TryToCreateTemporarySnapshot(originalDir)
	if err != nil {
		if required {
			return "", errors.Wrapf(err, "error creating filesystem snapshot for '%v'", originalDir)
		}

		if !errors.Is(err, fs_snapshot.ErrSnapshotFailedInPreviousAttempt) {
			s.manifest.FilesystemSnapshots = append(s.manifest.FilesystemSnapshots,
				snapshot.FilesystemSnapshotInfo{
					Path:      originalDir,
					Timestamp: clock.Now(),
					Error:     err.Error(),
				})

			s.log.Errorf("Error creating filesystem snapshot for '%v': %v. Ignoring and using original files.", originalDir, err)
		}

		return originalDir, nil
	}

	if !s.snapshotIDs[snapshotInfo.ID] {
		s.snapshotIDs[snapshotInfo.ID] = true

		s.manifest.FilesystemSnapshots = append(s.manifest.FilesystemSnapshots,
			snapshot.FilesystemSnapshotInfo{
				ID:         snapshotInfo.ID,
				Path:       snapshotInfo.OriginalDir,
				Timestamp:  snapshotInfo.CreationTime,
				Provider:   snapshotInfo.Provider.Name,
				Attributes: snapshotInfo.Attributes,
			})

		s.log.Infof("Created filesystem snapshot for '%v' using provider '%v' and mapped to '%v",
			snapshotInfo.OriginalDir, snapshotInfo.Provider.Name, snapshotInfo.SnapshotDir)
	}

	return snapshotDir, nil
}

func (s *fsSnapshot) init() error {
	snapshoter, err := fs_snapshot.NewSnapshoter(nil)
	if err != nil {
		//nolint:wrapcheck
		return err
	}

	backuper, err := snapshoter.StartBackup(nil)
	if err != nil {
		snapshoter.Close()

		//nolint:wrapcheck
		return err
	}

	s.backuper = backuper
	s.snapshoter = snapshoter

	return nil
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
