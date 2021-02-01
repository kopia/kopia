// Package robustness contains tests that that validate data stability over time.
// The package, while designed for Kopia, is written with abstractions that
// can be used to test other environments.
package robustness

// Snapshotter is an interface that describes methods
// for taking, restoring, deleting snapshots, and
// tracking them by a string snapshot ID.
type Snapshotter interface {
	CreateSnapshot(sourceDir string, opts map[string]string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string, opts map[string]string) error
	DeleteSnapshot(snapID string, opts map[string]string) error
	RunGC(opts map[string]string) error
	ListSnapshots() ([]string, error)
}
