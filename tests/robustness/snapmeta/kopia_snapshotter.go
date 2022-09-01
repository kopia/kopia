//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/tools/fswalker"
)

// KopiaSnapshotter wraps the functionality to connect to a kopia repository with
// the fswalker WalkCompare.
type KopiaSnapshotter struct {
	comparer *fswalker.WalkCompare
	kopiaConnector
}

// KopiaSnapshotter implements robustness.Snapshotter.
var _ robustness.Snapshotter = (*KopiaSnapshotter)(nil)

// NewSnapshotter returns a Kopia based Snapshotter.
// ConnectOrCreateRepo must be invoked to enable the interface.
func NewSnapshotter(baseDirPath string) (*KopiaSnapshotter, error) {
	ks := &KopiaSnapshotter{
		comparer: fswalker.NewWalkCompare(),
	}

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

// ConnectClient should be called by a client to connect itself to the server
// using the given cert fingerprint.
func (ks *KopiaSnapshotter) ConnectClient(fingerprint, user string) error {
	return ks.connectClient(fingerprint, user)
}

// DisconnectClient should be called by a client to disconnect itself from the server.
func (ks *KopiaSnapshotter) DisconnectClient(user string) {
	if err := ks.snap.DisconnectClient(); err != nil {
		log.Printf("Error disconnecting %s from server: %v\n", user, err)
	}
}

// AuthorizeClient should be called by a server to add a client to the server's
// user list.
func (ks *KopiaSnapshotter) AuthorizeClient(user string) error {
	return ks.authorizeClient(user)
}

// RemoveClient should be called by a server to remove a client from its user list.
func (ks *KopiaSnapshotter) RemoveClient(user string) {
	if err := ks.snap.RemoveClient(user, defaultHost); err != nil {
		log.Printf("Error removing %s from server: %v\n", user, err)
	}
}

// ServerCmd returns the server command.
func (ks *KopiaSnapshotter) ServerCmd() *exec.Cmd {
	return ks.serverCmd
}

// ServerFingerprint returns the cert fingerprint that is used to connect to the server.
func (ks *KopiaSnapshotter) ServerFingerprint() string {
	return ks.serverFingerprint
}

// CreateSnapshot is part of Snapshotter.
func (ks *KopiaSnapshotter) CreateSnapshot(ctx context.Context, sourceDir string, opts map[string]string) (snapID string, fingerprint []byte, snapStats *robustness.CreateSnapshotStats, err error) {
	fingerprint, err = ks.comparer.Gather(ctx, sourceDir, opts)
	if err != nil {
		return
	}

	ssStart := clock.Now()

	snapID, err = ks.snap.CreateSnapshot(sourceDir)
	if err != nil {
		return
	}

	ssEnd := clock.Now()

	snapStats = &robustness.CreateSnapshotStats{
		SnapStartTime: ssStart,
		SnapEndTime:   ssEnd,
	}

	return
}

// RestoreSnapshot restores the snapshot with the given ID to the provided restore directory. It returns
// fingerprint verification data of the restored snapshot directory.
func (ks *KopiaSnapshotter) RestoreSnapshot(ctx context.Context, snapID, restoreDir string, opts map[string]string) (fingerprint []byte, err error) {
	err = ks.snap.RestoreSnapshot(snapID, restoreDir)
	if err != nil {
		return
	}

	return ks.comparer.Gather(ctx, restoreDir, opts)
}

// RestoreSnapshotCompare restores the snapshot with the given ID to the provided restore directory, then verifies the data
// that has been restored against the provided fingerprint validation data.
func (ks *KopiaSnapshotter) RestoreSnapshotCompare(ctx context.Context, snapID, restoreDir string, validationData []byte, reportOut io.Writer, opts map[string]string) (err error) {
	err = ks.snap.RestoreSnapshot(snapID, restoreDir)
	if err != nil {
		return err
	}

	return ks.comparer.Compare(ctx, restoreDir, validationData, reportOut, opts)
}

// DeleteSnapshot is part of Snapshotter.
func (ks *KopiaSnapshotter) DeleteSnapshot(ctx context.Context, snapID string, opts map[string]string) error {
	return ks.snap.DeleteSnapshot(snapID)
}

// RunGC is part of Snapshotter.
func (ks *KopiaSnapshotter) RunGC(ctx context.Context, opts map[string]string) error {
	return ks.snap.RunGC()
}

// ListSnapshots is part of Snapshotter.
func (ks *KopiaSnapshotter) ListSnapshots(ctx context.Context) ([]string, error) {
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
	//nolint:nilnil
	return nil, nil
}

// ConnectOrCreateFilesystemWithServer TBD: remove this.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystemWithServer(serverAddr, repoPath string) (*exec.Cmd, error) {
	//nolint:nilnil
	return nil, nil
}

// Cleanup should be called before termination.
func (ks *KopiaSnapshotter) Cleanup() {
	ks.snap.Cleanup()
}

// GetRepositoryStatus returns the repository status in JSON format.
func (ks *KopiaSnapshotter) GetRepositoryStatus() (cli.RepositoryStatus, error) {
	var rs cli.RepositoryStatus

	a1, _, err := ks.snap.Run("repository", "status", "--json")
	if err != nil {
		return rs, err
	}

	if err := json.Unmarshal([]byte(a1), &rs); err != nil {
		return rs, err
	}

	return rs, nil
}

// UpgradeRepository upgrades the given kopia repository
// from current format version to latest stable format version.
func (ks *KopiaSnapshotter) UpgradeRepository() error {
	os.Setenv("KOPIA_UPGRADE_LOCK_ENABLED", "1")

	_, _, err := ks.snap.Run("repository", "upgrade",
		"--upgrade-owner-id", "robustness-tests",
		"--io-drain-timeout", "30s", "--allow-unsafe-upgrade",
		"--status-poll-interval", "1s")

	return err
}
