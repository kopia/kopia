// Package robustness contains tests that that validate data stability over time.
// The package, while designed for Kopia, is written with abstractions that
// can be used to test other environments.
package robustness

import (
	"io"
	"time"
)

// Snapshotter is an interface that describes methods
// for taking, restoring, deleting snapshots, and
// tracking them by a string snapshot ID.
type Snapshotter interface {
	DeleteSnapshot(snapID string, opts map[string]string) error
	RunGC(opts map[string]string) error
	ListSnapshots() ([]string, error)

	CreateSnapshot(sourceDir string, opts map[string]string) (snapID string, fingerprint []byte, stats *CreateSnapshotStats, err error)
	RestoreSnapshot(snapID, restoreDir string, opts map[string]string) ([]byte, error)
	RestoreSnapshotCompare(snapID, restoreDir string, validationData []byte, reportOut io.Writer, opts map[string]string) error
}

// CreateSnapshotStats is a struct for returning various stats from the snapshot execution.
type CreateSnapshotStats struct {
	SnapStartTime time.Time
	SnapEndTime   time.Time
	Raw           []byte
}
