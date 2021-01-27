// Package robustness contains tests that that validate data stability over time.
// The package, while designed for Kopia, is written with abstractions that
// can be used to test other environments.
package robustness

import "os/exec"

// Snapshotter is an interface that describes methods
// for taking, restoring, deleting snapshots, and
// tracking them by a string snapshot ID.
type Snapshotter interface {
	RepoManager // TBD: may not be needed once initialization refactored
	CreateSnapshot(sourceDir string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string) error
	DeleteSnapshot(snapID string) error
	RunGC() error
	ListSnapshots() ([]string, error)
	Run(args ...string) (stdout, stderr string, err error) // TBD: remove once initialization refactored
}

// RepoManager is an interface that describes connecting to
// a repository.
// TBD: may not be needed once initialization refactored.
type RepoManager interface {
	ConnectOrCreateS3(bucketName, pathPrefix string) error
	ConnectOrCreateFilesystem(path string) error
	ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) (*exec.Cmd, error)
	ConnectOrCreateFilesystemWithServer(serverAddr, repoPath string) (*exec.Cmd, error)
}
