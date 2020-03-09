// Package snap describes entities that are capable of performing
// common snapshot operations
package snap

// Snapshotter is an interface that describes methods
// for taking, restoring, deleting snapshots, and
// tracking them by a string snapshot ID.
type Snapshotter interface {
	RepoManager
	CreateSnapshot(sourceDir string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string) error
	DeleteSnapshot(snapID string) error
	ListSnapshots() ([]string, error)
	Run(args ...string) (stdout, stderr string, err error)
}

// RepoManager is an interface that describes connecting to
// a repository
type RepoManager interface {
	ConnectOrCreateS3(bucketName, pathPrefix string) error
	ConnectOrCreateFilesystem(path string) error
}
