package kopiarunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
)

const (
	contentCacheSizeMBFlag  = "--content-cache-size-mb"
	metadataCacheSizeMBFlag = "--metadata-cache-size-mb"
	noCheckForUpdatesFlag   = "--no-check-for-updates"
	noProgressFlag          = "--no-progress"
	parallelFlag            = "--parallel"
	retryCount              = 900
	retryInterval           = 1 * time.Second
	waitingForServerString  = "waiting for server to start"
	serverControlPassword   = "abcdef"

	// Flag value settings.
	contentCacheSizeSettingMB  = 500
	metadataCacheSizeSettingMB = 500
	parallelSetting            = 8

	aclEnabledMatchStr = "ACLs already enabled"
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
func (ks *KopiaSnapshotter) ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) (*exec.Cmd, string, error) {
	repoArgs := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}
	return ks.ConnectOrCreateRepoWithServer(serverAddr, repoArgs...)
}

// ConnectOrCreateFilesystemWithServer attempts to connect or create repo in local filesystem,
// but with TLS server/client Model.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystemWithServer(serverAddr, repoPath string) (*exec.Cmd, string, error) {
	repoArgs := []string{"filesystem", "--path", repoPath}
	return ks.ConnectOrCreateRepoWithServer(serverAddr, repoArgs...)
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
	stdOut, errOut, err := ks.Runner.Run("snapshot", "create", parallelFlag, strconv.Itoa(parallelSetting), noProgressFlag, source)
	if err != nil {
		return "", err
	}

	if stdOut != "" {
		return parseSnapID(strings.Split(stdOut, "\n"))
	}

	return parseSnapID(strings.Split(errOut, "\n"))
}

// RestoreSnapshot implements the Snapshotter interface, issues a kopia snapshot
// restore command of the provided snapshot ID to the provided restore destination.
func (ks *KopiaSnapshotter) RestoreSnapshot(snapID, restoreDir string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "restore", snapID, restoreDir)
	return err
}

// VerifySnapshot implements the Snapshotter interface to verify a kopia snapshot corruption
// verify command of args to the provided parameters such as --verify-files-percent.
func (ks *KopiaSnapshotter) VerifySnapshot(args ...string) (err error) {
	args = append([]string{"snapshot", "verify"}, args...)
	_, _, err = ks.Runner.Run(args...)

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
	_, _, err = ks.Runner.Run("maintenance", "run", "--full")
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
	args = append([]string{
		"server", "start",
		"--address", addr,
		"--server-control-password", serverControlPassword,
	}, args...)

	return ks.Runner.RunAsync(args...)
}

// AuthorizeClient adds a client to the server's user list.
func (ks *KopiaSnapshotter) AuthorizeClient(user, host string, args ...string) error {
	args = append([]string{
		"server", "user", "add",
		user + "@" + host,
		"--user-password", repoPassword,
	}, args...)
	_, _, err := ks.Runner.Run(args...)

	return err
}

// RemoveClient removes a client from the server's user list.
func (ks *KopiaSnapshotter) RemoveClient(user, host string, args ...string) error {
	args = append([]string{
		"server", "user", "delete",
		user + "@" + host,
	}, args...)
	_, _, err := ks.Runner.Run(args...)

	return err
}

// DisconnectClient should be called by a client to disconnect itself from the server.
func (ks *KopiaSnapshotter) DisconnectClient(args ...string) error {
	args = append([]string{"repo", "disconnect"}, args...)
	_, _, err := ks.Runner.Run(args...)

	return err
}

// RefreshServer refreshes the server at the given address.
func (ks *KopiaSnapshotter) RefreshServer(addr, fingerprint string, args ...string) error {
	addr = fmt.Sprintf("https://%v", addr)
	args = append([]string{
		"server", "refresh",
		"--address", addr,
		"--server-cert-fingerprint", fingerprint,
		"--server-control-password", serverControlPassword,
	}, args...)
	_, _, err := ks.Runner.Run(args...)

	return err
}

// ListClients lists the clients that are registered with the Kopia server.
func (ks *KopiaSnapshotter) ListClients(addr, fingerprint string, args ...string) error {
	args = append([]string{"server", "user", "list"}, args...)
	_, _, err := ks.Runner.Run(args...)

	return err
}

// ConnectClient connects a given client to the server at the given address using the
// given cert fingerprint.
func (ks *KopiaSnapshotter) ConnectClient(addr, fingerprint, user, host string, args ...string) error {
	addr = fmt.Sprintf("https://%v", addr)
	args = append([]string{
		"repo", "connect", "server",
		"--url", addr,
		"--server-cert-fingerprint", fingerprint,
		"--override-username", user,
		"--override-hostname", host,
	}, args...)
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
			if len(spl) == 2 { //nolint:mnd
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
func (ks *KopiaSnapshotter) waitUntilServerStarted(ctx context.Context, addr, fingerprint string, serverStatusArgs ...string) error {
	statusArgs := append([]string{
		"server", "status",
		"--address", addr,
		"--server-cert-fingerprint", fingerprint,
		"--server-control-password", serverControlPassword,
	}, serverStatusArgs...)

	if err := retry.PeriodicallyNoValue(ctx, retryInterval, retryCount, waitingForServerString, func() error {
		_, _, err := ks.Runner.Run(statusArgs...)
		return err
	}, retry.Always); err != nil {
		return errors.New("server failed to start")
	}

	return nil
}

// ConnectOrCreateRepoWithServer creates Repository and a TLS server/client model for interaction.
func (ks *KopiaSnapshotter) ConnectOrCreateRepoWithServer(serverAddr string, args ...string) (*exec.Cmd, string, error) {
	if err := ks.ConnectOrCreateRepo(args...); err != nil {
		return nil, "", err
	}

	var tempDir string

	var tempDirErr error

	if tempDir, tempDirErr = os.MkdirTemp("", "kopia"); tempDirErr != nil {
		return nil, "", tempDirErr
	}

	defer os.RemoveAll(tempDir)

	tlsCertFile := filepath.Join(tempDir, "kopiaserver.cert")
	tlsKeyFile := filepath.Join(tempDir, "kopiaserver.key")

	serverArgs := []string{"--tls-generate-cert", "--tls-cert-file", tlsCertFile, "--tls-key-file", tlsKeyFile}

	var cmd *exec.Cmd

	var cmdErr error

	if cmd, cmdErr = ks.CreateServer(serverAddr, serverArgs...); cmdErr != nil {
		return nil, "", errors.Wrap(cmdErr, "CreateServer failed")
	}

	if err := certKeyExist(context.TODO(), tlsCertFile, tlsKeyFile); err != nil {
		if buf, ok := cmd.Stderr.(*bytes.Buffer); ok {
			// If the STDERR buffer does not contain any obvious error output,
			// it is possible the async server creation above is taking a long time
			// to open the repository, and we timed out waiting for it to write the TLS certs.
			log.Print("failure in certificate generation:", buf.String())
		}

		return nil, "", err
	}

	var fingerprint string

	var fingerprintError error

	if fingerprint, fingerprintError = getFingerPrintFromCert(tlsCertFile); fingerprintError != nil {
		return nil, "", fingerprintError
	}

	serverAddr = fmt.Sprintf("https://%v", serverAddr)
	if err := ks.waitUntilServerStarted(context.TODO(), serverAddr, fingerprint); err != nil {
		return cmd, "", err
	}

	// Enable ACL and add a rule to allow all clients to access all snapshots
	err := ks.setServerPermissions()

	return cmd, fingerprint, err
}

func (ks *KopiaSnapshotter) setServerPermissions(args ...string) error {
	runArgs := append([]string{"server", "acl", "enable"}, args...)

	// Return early if ACL is already enabled, assuming permissions have already
	// been set on previous runs.
	_, stdErr, err := ks.Runner.Run(runArgs...)
	if errIsACLEnabled(stdErr) {
		return nil
	}

	if err != nil {
		return err
	}

	// Allow all clients to read all snapshots
	runArgs = append([]string{
		"server", "acl", "add",
		"--user", "*@*",
		"--access", "FULL",
		"--target", "type=snapshot",
	}, args...)

	_, _, err = ks.Runner.Run(runArgs...)
	if err != nil {
		return err
	}

	runArgs = append([]string{"server", "acl", "list"}, args...)
	_, _, err = ks.Runner.Run(runArgs...)

	return err
}

func getFingerPrintFromCert(path string) (string, error) {
	pemData, err := os.ReadFile(path) //nolint:gosec
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

func errIsACLEnabled(stdErr string) bool {
	return strings.Contains(stdErr, aclEnabledMatchStr)
}
