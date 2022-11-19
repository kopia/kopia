// Package filesystemsnapshots implements support for filesystem snapshots based on mappedfs.
package filesystemsnapshots

import (
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/pescuma/go-fs-snapshot/lib/fs_snapshot"

	"github.com/kopia/kopia/fs/mappedfs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type fsSnapshotsMapper struct {
	getOrCreateBackuper func() (fs_snapshot.Backuper, error)
	log                 logging.Logger
	policyTree          *policy.Tree
	manifest            *snapshot.Manifest
	root                string
	snapshotIDs         map[string]bool
}

// Apply creates a filesystem snapshot if configured in the policies.
func (s *fsSnapshotsMapper) Apply(originalDir string) (string, error) {
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

	// Store the error once per kopia snapshot
	backuper, err := s.getOrCreateBackuper()
	if err != nil {
		if required {
			return "", errors.Wrapf(err, "error creating filesystem snapshot engine")
		}

		if !hasError(s.manifest.FilesystemSnapshots, err.Error()) {
			s.manifest.FilesystemSnapshots = append(s.manifest.FilesystemSnapshots,
				snapshot.FilesystemSnapshotInfo{
					Path:      originalDir,
					Timestamp: clock.Now(),
					Error:     err.Error(),
				})

			s.log.Errorf("Error creating filesystem snapshot engine: %v. Ignoring and using original files.", err)
		}

		return originalDir, nil
	}

	snapshotDir, snapshotInfo, err := backuper.TryToCreateTemporarySnapshot(originalDir)
	if err != nil {
		if required {
			return "", errors.Wrapf(err, "error creating filesystem snapshot for '%v'", originalDir)
		}

		if !hasError(s.manifest.FilesystemSnapshots, err.Error()) {
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

func hasError(fsis []snapshot.FilesystemSnapshotInfo, err string) bool {
	for _, fsi := range fsis {
		if fsi.Error == err {
			return true
		}
	}

	return false
}

// Close frees resources.
func (s *fsSnapshotsMapper) Close() {
}

var _ mappedfs.FilesystemMapper = &fsSnapshotsMapper{}
