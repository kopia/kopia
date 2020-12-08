package kopiarunner

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/tests/robustness/snap"
)

var _ snap.Snapshotter = &KopiaSnapshotter{}

const (
	contentCacheSizeMBFlag  = "--content-cache-size-mb"
	metadataCacheSizeMBFlag = "--metadata-cache-size-mb"
	noCheckForUpdatesFlag   = "--no-check-for-updates"
	noProgressFlag          = "--no-progress"
	parallelFlag            = "--parallel"
	retryCount              = 180
	retryInterval           = 1 * time.Second
	waitingForServerString  = "waiting for server to start"

	// Flag value settings.
	contentCacheSizeSettingMB  = 500
	metadataCacheSizeSettingMB = 500
	parallelSetting            = 8
)

// KopiaSnapshotter implements the Snapshotter interface using Kopia commands.
type KopiaSnapshotter struct {
	Runner *Runner
}

// NewKopiaSnapshotter instantiates a new KopiaSnapshotter and returns its pointer.
func NewKopiaSnapshotter(baseDir string) (*KopiaSnapshotter, error) {
	runner, err := NewRunner(baseDir)
	if err != nil {
		return nil, err
	}

	return &KopiaSnapshotter{
		Runner: runner,
	}, nil
}

// Cleanup cleans up the kopia Runner.
func (ks *KopiaSnapshotter) Cleanup() {
	if ks.Runner != nil {
		ks.Runner.Cleanup()
	}
}

func (ks *KopiaSnapshotter) repoConnectCreate(op string, args ...string) error {
	args = append([]string{"repo", op}, args...)

	args = append(args,
		contentCacheSizeMBFlag, strconv.Itoa(contentCacheSizeSettingMB),
		metadataCacheSizeMBFlag, strconv.Itoa(metadataCacheSizeSettingMB),
		noCheckForUpdatesFlag,
	)

	_, _, err := ks.Runner.Run(args...)

	return err
}

// CreateRepo creates a kopia repository with the provided arguments.
func (ks *KopiaSnapshotter) CreateRepo(args ...string) (err error) {
	return ks.repoConnectCreate("create", args...)
}

// ConnectRepo connects to the repository described by the provided arguments.
func (ks *KopiaSnapshotter) ConnectRepo(args ...string) (err error) {
	return ks.repoConnectCreate("connect", args...)
}

// ConnectOrCreateRepo attempts to connect to a repo described by the provided
// arguments, and attempts to create it if connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateRepo(args ...string) error {
	err := ks.ConnectRepo(args...)
	if err == nil {
		return nil
	}

	return ks.CreateRepo(args...)
}

// ConnectOrCreateS3 attempts to connect to a kopia repo in the s3 bucket identified
// by the provided bucketName, at the provided path prefix. It will attempt to
// create one there if connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}

	return ks.ConnectOrCreateRepo(args...)
}

// ConnectOrCreateS3WithServer attempts to connect or create S3 bucket, but with TLS client/server Model.
func (ks *KopiaSnapshotter) ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) (*exec.Cmd, error) {
	repoArgs := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}
	return ks.createAndConnectServer(serverAddr, repoArgs...)
}

// ConnectOrCreateFilesystemWithServer attempts to connect or create repo in local filesystem,
// but with TLS server/client Model.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystemWithServer(serverAddr, repoPath string) (*exec.Cmd, error) {
	repoArgs := []string{"filesystem", "--path", repoPath}
	return ks.createAndConnectServer(serverAddr, repoArgs...)
}

// ConnectOrCreateFilesystem attempts to connect to a kopia repo in the local
// filesystem at the path provided. It will attempt to create one there if
// connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystem(repoPath string) error {
	args := []string{"filesystem", "--path", repoPath}

	return ks.ConnectOrCreateRepo(args...)
}

// CreateSnapshot implements the Snapshotter interface, issues a kopia snapshot
// create command on the provided source path.
func (ks *KopiaSnapshotter) CreateSnapshot(source string) (snapID string, err error) {
	_, errOut, err := ks.Runner.Run("snapshot", "create", parallelFlag, strconv.Itoa(parallelSetting), noProgressFlag, source)
	if err != nil {
		return "", err
	}

	return parseSnapID(strings.Split(errOut, "\n"))
}

// RestoreSnapshot implements the Snapshotter interface, issues a kopia snapshot
// restore command of the provided snapshot ID to the provided restore destination.
func (ks *KopiaSnapshotter) RestoreSnapshot(snapID, restoreDir string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "restore", snapID, restoreDir)
	return err
}

// DeleteSnapshot implements the Snapshotter interface, issues a kopia snapshot
// delete of the provided snapshot ID.
func (ks *KopiaSnapshotter) DeleteSnapshot(snapID string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "delete", snapID, "--delete")
	return err
}

// RunGC implements the Snapshotter interface, issues a gc command to the kopia repo.
func (ks *KopiaSnapshotter) RunGC() (err error) {
	_, _, err = ks.Runner.Run("snapshot", "gc")
	return err
}

// ListSnapshots implements the Snapshotter interface, issues a kopia snapshot
// list and parses the snapshot IDs.
func (ks *KopiaSnapshotter) ListSnapshots() ([]string, error) {
	snapIDListMan, err := ks.snapIDsFromManifestList()
	if err != nil {
		return nil, err
	}

	// Validate the list against kopia snapshot list --all
	snapIDListSnap, err := ks.snapIDsFromSnapListAll()
	if err != nil {
		return nil, err
	}

	if got, want := len(snapIDListSnap), len(snapIDListMan); got != want {
		return nil, errors.Errorf("Snapshot list len (%d) does not match manifest list len (%d)", got, want)
	}

	return snapIDListMan, nil
}

func (ks *KopiaSnapshotter) snapIDsFromManifestList() ([]string, error) {
	stdout, _, err := ks.Runner.Run("manifest", "list")
	if err != nil {
		return nil, errors.Wrap(err, "failure during kopia manifest list")
	}

	return parseManifestListForSnapshotIDs(stdout), nil
}

func (ks *KopiaSnapshotter) snapIDsFromSnapListAll() ([]string, error) {
	// Validate the list against kopia snapshot list --all
	stdout, _, err := ks.Runner.Run("snapshot", "list", "--all", "--manifest-id", "--show-identical")
	if err != nil {
		return nil, errors.Wrap(err, "failure during kopia snapshot list")
	}

	return parseSnapshotListForSnapshotIDs(stdout), nil
}

// Run implements the Snapshotter interface, issues an arbitrary kopia command and returns
// the output.
func (ks *KopiaSnapshotter) Run(args ...string) (stdout, stderr string, err error) {
	return ks.Runner.Run(args...)
}

// CreateServer creates a new instance of Kopia Server with provided address.
func (ks *KopiaSnapshotter) CreateServer(addr string, args ...string) (*exec.Cmd, error) {
	args = append([]string{"server", "start", "--address", addr}, args...)

	return ks.Runner.RunAsync(args...)
}

// ConnectServer creates a new client, and connect it to Kopia Server with provided address.
func (ks *KopiaSnapshotter) ConnectServer(addr string, args ...string) error {
	args = append([]string{"repo", "connect", "server", "--url", addr}, args...)
	_, _, err := ks.Runner.Run(args...)

	return err
}

func parseSnapID(lines []string) (string, error) {
	pattern := regexp.MustCompile(`Created snapshot with root \S+ and ID (\S+)`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1], nil
		}
	}

	return "", errors.New("snap ID could not be parsed")
}

func parseSnapshotListForSnapshotIDs(output string) []string {
	var ret []string

	lines := strings.Split(output, "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		for _, f := range fields {
			spl := strings.Split(f, "manifest:")
			if len(spl) == 2 { //nolint:gomnd
				ret = append(ret, spl[1])
			}
		}
	}

	return ret
}

func parseManifestListForSnapshotIDs(output string) []string {
	var ret []string

	lines := strings.Split(output, "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		typeFieldIdx := 5
		if len(fields) > typeFieldIdx {
			if fields[typeFieldIdx] == "type:snapshot" {
				ret = append(ret, fields[0])
			}
		}
	}

	return ret
}

// waitUntilServerStarted returns error if the Kopia API server fails to start before timeout.
func (ks *KopiaSnapshotter) waitUntilServerStarted(ctx context.Context, addr string, serverStatusArgs ...string) error {
	statusArgs := append([]string{"server", "status", "--address", addr}, serverStatusArgs...)

	if err := retry.PeriodicallyNoValue(ctx, retryInterval, retryCount, waitingForServerString, func() error {
		_, _, err := ks.Runner.Run(statusArgs...)
		return err
	}, retry.Always); err != nil {
		return errors.New("server failed to start")
	}

	return nil
}

// createAndConnectServer creates Repository and a TLS server/client model for interaction.
func (ks *KopiaSnapshotter) createAndConnectServer(serverAddr string, args ...string) (*exec.Cmd, error) {
	if err := ks.ConnectOrCreateRepo(args...); err != nil {
		return nil, err
	}

	var tempDir string

	var tempDirErr error

	if tempDir, tempDirErr = ioutil.TempDir("", "kopia"); tempDirErr != nil {
		return nil, tempDirErr
	}

	defer os.RemoveAll(tempDir) // nolint:errcheck

	tlsCertFile := filepath.Join(tempDir, "kopiaserver.cert")
	tlsKeyFile := filepath.Join(tempDir, "kopiaserver.key")

	serverArgs := []string{"--tls-generate-cert", "--tls-cert-file", tlsCertFile, "--tls-key-file", tlsKeyFile}

	var cmd *exec.Cmd

	var cmdErr error

	if cmd, cmdErr = ks.CreateServer(serverAddr, serverArgs...); cmdErr != nil {
		return nil, cmdErr
	}

	if err := certKeyExist(context.TODO(), tlsCertFile, tlsKeyFile); err != nil {
		return nil, err
	}

	var fingerprint string

	var fingerprintError error

	if fingerprint, fingerprintError = getFingerPrintFromCert(tlsCertFile); fingerprintError != nil {
		return nil, fingerprintError
	}

	serverAddr = fmt.Sprintf("https://%v", serverAddr)

	if err := ks.waitUntilServerStarted(context.TODO(), serverAddr, "--server-cert-fingerprint", fingerprint); err != nil {
		return cmd, err
	}

	clientArgs := []string{"--server-cert-fingerprint", fingerprint}
	if err := ks.ConnectServer(serverAddr, clientArgs...); err != nil {
		return nil, err
	}

	return cmd, nil
}

func getFingerPrintFromCert(path string) (string, error) {
	pemData, err := ioutil.ReadFile(path) //nolint:gosec
	if err != nil {
		return "", err
	}

	block, rest := pem.Decode([]byte(pemData)) //nolint:unconvert
	if block == nil || len(rest) > 0 {
		return "", errors.New("pem decoding error")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", err
	}

	fingerprint := sha256.Sum256(cert.Raw)

	return hex.EncodeToString(fingerprint[:]), nil
}

func certKeyExist(ctx context.Context, tlsCertFile, tlsKeyFile string) error {
	if err := retry.PeriodicallyNoValue(ctx, retryInterval, retryCount, "waiting for server to start", func() error {
		if _, err := os.Stat(tlsCertFile); os.IsNotExist(err) {
			return err
		}
		if _, err := os.Stat(tlsKeyFile); os.IsNotExist(err) {
			return err
		}
		return nil
	}, retry.Always); err != nil {
		return errors.New("unable to find TLS Certs")
	}

	return nil
}
