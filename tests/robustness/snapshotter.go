//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package robustness contains tests that validate data stability over time.
// The package, while designed for Kopia, is written with abstractions that
// can be used to test other environments.
package robustness

import (
	"context"
	"io"
	"time"
)

// Snapshotter is an interface that describes methods
// for taking, restoring, deleting snapshots, and
// tracking them by a string snapshot ID.
type Snapshotter interface {
	CreateSnapshot(ctx context.Context, sourceDir string, opts map[string]string) (snapID string, fingerprint []byte, stats *CreateSnapshotStats, err error)
	RestoreSnapshot(ctx context.Context, snapID, restoreDir string, opts map[string]string) ([]byte, error)
	RestoreSnapshotCompare(ctx context.Context, snapID, restoreDir string, validationData []byte, reportOut io.Writer, opts map[string]string) error
	DeleteSnapshot(ctx context.Context, snapID string, opts map[string]string) error
	RunGC(ctx context.Context, opts map[string]string) error
	ListSnapshots(ctx context.Context) ([]string, error)
}

// CreateSnapshotStats is a struct for returning various stats from the snapshot execution.
type CreateSnapshotStats struct {
	SnapStartTime time.Time
	SnapEndTime   time.Time
	Raw           []byte
}
