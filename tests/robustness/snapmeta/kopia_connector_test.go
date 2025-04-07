//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package snapmeta

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKopiaConnector(t *testing.T) {
	require := require.New(t) //nolint:gocritic

	t.Setenv("KOPIA_EXE", "kopia.exe")

	tc := &testConnector{}

	err := tc.initializeConnector("")
	require.NoError(err)
	require.NotNil(tc.snap)
	require.NotNil(tc.initS3Fn)
	require.NotNil(tc.initS3WithServerFn)
	require.NotNil(tc.initFilesystemFn)
	require.NotNil(tc.initFilesystemWithServerFn)

	tc.initS3Fn = tc.testInitS3
	tc.initFilesystemFn = tc.testInitFilesystem
	tc.initS3WithServerFn = tc.testInitS3WithServer
	tc.initFilesystemWithServerFn = tc.testInitFilesystemWithServer

	repoPath := "repoPath"
	bucketName := "bucketName"

	t.Setenv(EngineModeEnvKey, EngineModeBasic)
	t.Setenv(S3BucketNameEnvKey, "")
	tc.reset()
	require.NoError(tc.connectOrCreateRepo(repoPath))
	require.True(tc.initFilesystemCalled)
	require.Equal(repoPath, tc.tcRepoPath)

	t.Setenv(EngineModeEnvKey, EngineModeBasic)
	t.Setenv(S3BucketNameEnvKey, bucketName)
	tc.reset()
	require.NoError(tc.connectOrCreateRepo(repoPath))
	require.True(tc.initS3Called)
	require.Equal(repoPath, tc.tcRepoPath)
	require.Equal(bucketName, tc.tcBucketName)

	t.Setenv(EngineModeEnvKey, EngineModeServer)
	t.Setenv(S3BucketNameEnvKey, "")
	tc.reset()
	require.NoError(tc.connectOrCreateRepo(repoPath))
	require.True(tc.initFilesystemWithServerCalled)
	require.Equal(repoPath, tc.tcRepoPath)
	require.Equal(defaultAddr, tc.tcAddr)

	t.Setenv(EngineModeEnvKey, EngineModeServer)
	t.Setenv(S3BucketNameEnvKey, bucketName)
	tc.reset()
	require.NoError(tc.connectOrCreateRepo(repoPath))
	require.True(tc.initS3WithServerCalled)
	require.Equal(repoPath, tc.tcRepoPath)
	require.Equal(bucketName, tc.tcBucketName)
	require.Equal(defaultAddr, tc.tcAddr)
}

type testConnector struct {
	kopiaConnector
	tcRepoPath                     string
	tcBucketName                   string
	tcAddr                         string
	initS3Called                   bool
	initFilesystemCalled           bool
	initS3WithServerCalled         bool
	initFilesystemWithServerCalled bool
}

func (tc *testConnector) reset() {
	tc.tcRepoPath = ""
	tc.tcBucketName = ""
	tc.tcAddr = ""
	tc.initS3Called = false
	tc.initFilesystemCalled = false
	tc.initS3WithServerCalled = false
	tc.initFilesystemWithServerCalled = false
}

func (tc *testConnector) testInitS3(repoPath, bucketName string) error {
	tc.tcRepoPath = repoPath
	tc.tcBucketName = bucketName
	tc.initS3Called = true

	return nil
}

func (tc *testConnector) testInitFilesystem(repoPath string) error {
	tc.tcRepoPath = repoPath
	tc.initFilesystemCalled = true

	return nil
}

func (tc *testConnector) testInitS3WithServer(repoPath, bucketName, addr string) error {
	tc.tcRepoPath = repoPath
	tc.tcBucketName = bucketName
	tc.tcAddr = addr
	tc.initS3WithServerCalled = true

	return nil
}

func (tc *testConnector) testInitFilesystemWithServer(repoPath, addr string) error {
	tc.tcRepoPath = repoPath
	tc.tcAddr = addr
	tc.initFilesystemWithServerCalled = true

	return nil
}
