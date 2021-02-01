package snapmeta

import (
	"os/exec"
	"strconv"

	"github.com/kopia/kopia/tests/robustness"
)

// KopiaSnapshotter implements robustness.Snapshotter.
type KopiaSnapshotter struct {
	kopiaConnector
}

var _ robustness.Snapshotter = (*KopiaSnapshotter)(nil)

// NewSnapshotter returns a Kopia based Snapshotter.
// ConnectOrCreateRepo must be invoked to enable the interface.
func NewSnapshotter(baseDirPath string) (*KopiaSnapshotter, error) {
	ks := &KopiaSnapshotter{}

	if err := ks.initializeConnector(baseDirPath); err != nil {
		return nil, err
	}

	return ks, nil
}

// ConnectOrCreateRepo makes the Snapshotter ready for use.
func (ks *KopiaSnapshotter) ConnectOrCreateRepo(repoPath string) error {
	if err := ks.connectOrCreateRepo(repoPath); err != nil {
		return err
	}

	_, _, err := ks.snap.Run("policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1), "--compression", "s2-default")

	return err
}

// ServerCmd returns the server command.
func (ks *KopiaSnapshotter) ServerCmd() *exec.Cmd {
	return ks.serverCmd
}

// CreateSnapshot is part of Snapshotter.
func (ks *KopiaSnapshotter) CreateSnapshot(sourceDir string, opts map[string]string) (snapID string, err error) {
	return ks.snap.CreateSnapshot(sourceDir)
}

// RestoreSnapshot is part of Snapshotter.
func (ks *KopiaSnapshotter) RestoreSnapshot(snapID, restoreDir string, opts map[string]string) error {
	return ks.snap.RestoreSnapshot(snapID, restoreDir)
}

// DeleteSnapshot is part of Snapshotter.
func (ks *KopiaSnapshotter) DeleteSnapshot(snapID string, opts map[string]string) error {
	return ks.snap.DeleteSnapshot(snapID)
}

// RunGC is part of Snapshotter.
func (ks *KopiaSnapshotter) RunGC(opts map[string]string) error {
	return ks.snap.RunGC()
}

// ListSnapshots is part of Snapshotter.
func (ks *KopiaSnapshotter) ListSnapshots() ([]string, error) {
	return ks.snap.ListSnapshots()
}

// Run is part of Snapshotter.
func (ks *KopiaSnapshotter) Run(args ...string) (stdout, stderr string, err error) {
	return ks.snap.Run(args...)
}

// ConnectOrCreateS3 TBD: remove this.
func (ks *KopiaSnapshotter) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	return nil
}

// ConnectOrCreateFilesystem TBD: remove this.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystem(path string) error {
	return nil
}

// ConnectOrCreateS3WithServer TBD: remove this.
func (ks *KopiaSnapshotter) ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) (*exec.Cmd, error) {
	return nil, nil
}

// ConnectOrCreateFilesystemWithServer TBD: remove this.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystemWithServer(serverAddr, repoPath string) (*exec.Cmd, error) {
	return nil, nil
}

// Cleanup should be called before termination.
func (ks *KopiaSnapshotter) Cleanup() {
	ks.snap.Cleanup()
}
