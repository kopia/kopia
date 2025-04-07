//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"os"
	"os/exec"

	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

const (
	// S3BucketNameEnvKey is the environment variable required to connect to a repo on S3.
	S3BucketNameEnvKey = "S3_BUCKET_NAME"
	// EngineModeEnvKey is the environment variable required to switch between basic and server/client model.
	EngineModeEnvKey = "ENGINE_MODE"
	// EngineModeBasic is a constant used to check the engineMode.
	EngineModeBasic = "BASIC"
	// EngineModeServer is a constant used to check the engineMode.
	EngineModeServer = "SERVER"
	// defaultAddr is used for setting the address of Kopia Server.
	defaultAddr = "localhost:51515"
	// defaultHost is used for setting the address of Kopia Server.
	defaultHost = "robustness-host"
)

// kopiaConnector is a base type for Persister and Snapshotter.
// It provides a kopiarunner.KopiaSnapshotter and common initialization
// behavior based on the values of the EngineModeEnvKey and
// S3BucketNameEnvKey environment variables.
//
// Derived types can customize the initialization behavior by overriding
// the default handler functions.
type kopiaConnector struct {
	// properties set by initializeConnector()
	snap                       *kopiarunner.KopiaSnapshotter
	initS3Fn                   func(repoPath, bucketName string) error
	initS3WithServerFn         func(repoPath, bucketName, addr string) error
	initFilesystemFn           func(repoPath string) error
	initFilesystemWithServerFn func(repoPath, addr string) error

	// properties that may be set by connectOrCreateRepo()
	serverCmd         *exec.Cmd
	serverFingerprint string
}

// initializeConnector initializes the connector object and enables use of the
// connectOrCreateRepo method.
func (ki *kopiaConnector) initializeConnector(baseDirPath string) error {
	snap, err := kopiarunner.NewKopiaSnapshotter(baseDirPath)
	if err != nil {
		return err
	}

	ki.snap = snap
	ki.initS3Fn = ki.initS3
	ki.initFilesystemFn = ki.initFilesystem
	ki.initS3WithServerFn = ki.initS3WithServer
	ki.initFilesystemWithServerFn = ki.initFilesystemWithServer

	return nil
}

// connectOrCreateRepo makes the connector ready for use.
// It invokes the appropriate initialization routine based on the environment variables set.
func (ki *kopiaConnector) connectOrCreateRepo(repoPath string) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	engineMode := os.Getenv(EngineModeEnvKey)

	switch {
	case bucketName != "" && engineMode == EngineModeBasic:
		return ki.initS3Fn(repoPath, bucketName)

	case bucketName != "" && engineMode == EngineModeServer:
		return ki.initS3WithServerFn(repoPath, bucketName, defaultAddr)

	case bucketName == "" && engineMode == EngineModeServer:
		return ki.initFilesystemWithServerFn(repoPath, defaultAddr)

	default:
		return ki.initFilesystemFn(repoPath)
	}
}

// initS3 initializes basic mode with an S3 repository.
func (ki *kopiaConnector) initS3(repoPath, bucketName string) error {
	return ki.snap.ConnectOrCreateS3(bucketName, repoPath)
}

// initFilesystem initializes basic mode with a filesystem repository.
func (ki *kopiaConnector) initFilesystem(repoPath string) error {
	return ki.snap.ConnectOrCreateFilesystem(repoPath)
}

// initS3WithServer initializes server mode with an S3 repository.
func (ki *kopiaConnector) initS3WithServer(repoPath, bucketName, addr string) error {
	cmd, fingerprint, err := ki.snap.ConnectOrCreateS3WithServer(addr, bucketName, repoPath)
	ki.serverCmd = cmd
	ki.serverFingerprint = fingerprint

	return err
}

// initFilesystemWithServer initializes server mode with a filesystem repository.
func (ki *kopiaConnector) initFilesystemWithServer(repoPath, addr string) error {
	cmd, fingerprint, err := ki.snap.ConnectOrCreateFilesystemWithServer(addr, repoPath)
	ki.serverCmd = cmd
	ki.serverFingerprint = fingerprint

	return err
}

func (ki *kopiaConnector) authorizeClient(user string) error {
	if err := ki.snap.AuthorizeClient(user, defaultHost); err != nil {
		return err
	}

	if err := ki.snap.RefreshServer(defaultAddr, ki.serverFingerprint); err != nil {
		return err
	}

	err := ki.snap.ListClients(defaultAddr, ki.serverFingerprint)

	return err
}

func (ki *kopiaConnector) connectClient(fingerprint, user string) error {
	return ki.snap.ConnectClient(defaultAddr, fingerprint, user, defaultHost)
}
