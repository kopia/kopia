package endtoend_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/servertesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

// foo@bar - password baz.
var htpasswdFileContents = []byte("foo@bar:$2y$05$JWrExvBe5Knh0.AMLk5WHu.EzfOP.LhrqMIRf1YseZ/rulBjKqGJ.\n")

const (
	uiUsername = "ui-user-password"
	uiPassword = "ui-password"

	controlUsername = "control-user-password"
	controlPassword = "control-password"
)

func TestAPIServerRepository_htpasswd(t *testing.T) {
	t.Parallel()

	testAPIServerRepository(t, false)
}

func TestAPIServerRepository_RepositoryUsers(t *testing.T) {
	t.Parallel()

	testAPIServerRepository(t, true)
}

//nolint:thelper
func testAPIServerRepository(t *testing.T, allowRepositoryUsers bool) {
	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create 5 snapshots as foo@bar
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-username", "foo", "--override-hostname", "bar")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e1.RunAndExpectSuccess(t, "repo", "disconnect")

	// create one snapshot as not-foo@bar
	e1.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir, "--override-username", "not-foo", "--override-hostname", "bar")
	e1.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var pBlobsBefore, qBlobsBefore []blob.Metadata

	testutil.MustParseJSONLines(t, e1.RunAndExpectSuccess(t, "blob", "list", "--prefix=p", "--json"), &pBlobsBefore)
	testutil.MustParseJSONLines(t, e1.RunAndExpectSuccess(t, "blob", "list", "--prefix=q", "--json"), &qBlobsBefore)

	originalPBlobCount := len(pBlobsBefore)
	originalQBlobCount := len(qBlobsBefore)

	tlsCert := filepath.Join(e.ConfigDir, "tls.cert")
	tlsKey := filepath.Join(e.ConfigDir, "tls.key")

	var serverStartArgs []string

	if allowRepositoryUsers {
		e.RunAndExpectSuccess(t, "server", "users", "add", "foo@bar", "--user-password", "baz")
	} else {
		htpasswordFile := filepath.Join(e.ConfigDir, "htpasswd.txt")
		os.WriteFile(htpasswordFile, htpasswdFileContents, 0o755)
		serverStartArgs = append(serverStartArgs, "--htpasswd-file", htpasswordFile)
	}

	var sp testutil.ServerParameters

	e.SetLogOutput(true, "<first> ")

	t.Logf("******** first server startup ********")

	wait, _ := e.RunAndProcessStderr(t, sp.ProcessOutput,
		append([]string{
			"server", "start",
			"--address=localhost:0",
			"--tls-generate-cert",
			"--tls-key-file", tlsKey,
			"--tls-cert-file", tlsCert,
			"--server-username", uiUsername,
			"--server-password", uiPassword,
			"--server-control-username", controlUsername,
			"--server-control-password", controlPassword,
			"--shutdown-grace-period", "100ms",
		}, serverStartArgs...)...)

	t.Logf("detected server parameters %#v", sp)

	controlClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            controlUsername,
		Password:                            controlPassword,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)

	waitUntilServerStarted(ctx, t, controlClient)

	t.Logf("******** first server completed startup ********")

	// open repository client.
	ctx2, cancel := context.WithCancel(ctx)
	rep, err := servertesting.ConnectAndOpenAPIServer(t, ctx2, &repo.APIServerInfo{
		BaseURL:                             sp.BaseURL,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
	}, repo.ClientOptions{
		Username: "foo",
		Hostname: "bar",
	}, content.CachingOptions{}, "baz", &repo.Options{})

	// cancel immediately to ensure we did not spawn goroutines that depend on ctx inside
	// repo.OpenAPIServer()
	cancel()

	require.NoError(t, err)

	// open new write session in repository client

	_, writeSess, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "some writer"})
	require.NoError(t, err)

	defer writeSess.Close(ctx)

	t.Logf("******** server shutdown ********")
	require.NoError(t, serverapi.Shutdown(ctx, controlClient))
	// wait for the server to wind down.
	wait()
	t.Logf("******** finished server shutdown ********")

	defer rep.Close(ctx)

	e.SetLogOutput(true, "<second> ")

	// start the server again, using the same address & TLS key+cert, so existing connection
	// should be re-established.
	wait2, _ := e.RunAndProcessStderr(t, sp.ProcessOutput,
		append([]string{
			"server", "start",
			"--address=" + sp.BaseURL,
			"--tls-key-file", tlsKey,
			"--tls-cert-file", tlsCert,
			"--server-username", uiUsername,
			"--server-password", uiPassword,
			"--server-control-username", controlUsername,
			"--server-control-password", controlPassword,
		}, serverStartArgs...)...)

	t.Logf("detected server parameters %#v", sp)

	waitUntilServerStarted(ctx, t, controlClient)

	someLabels := map[string]string{
		"type":     "snapshot",
		"username": "foo",
		"hostname": "bar",
	}

	// invoke some read method, the repository will automatically reconnect to the server.
	// verify different page sizes (only works with GRPC).
	for _, pageSize := range []int32{0, 1, 3, 5, 6} {
		verifyFindManifestCount(ctx, t, rep, pageSize, someLabels, 5)
	}

	// the same method on a GRPC write session should fail because the stream was broken.
	_, err = writeSess.FindManifests(ctx, someLabels)
	require.Error(t, err)

	runner2 := testenv.NewInProcRunner(t)
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner2)
	e2.SetLogOutput(true, "<client2>")

	defer e2.RunAndExpectSuccess(t, "repo", "disconnect")

	e2.RunAndExpectSuccess(t,
		"repo", "connect", "server",
		"--url", sp.BaseURL+"/",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
		"--override-username", "foo",
		"--override-hostname", "bar",
		"--password", "baz",
	)

	// we are providing custom password to connect, make sure we won't be providing
	// (different) default password via environment variable, as command-line password
	// takes precedence over persisted password.
	delete(e2.Environment, "KOPIA_PASSWORD")

	// should see one snapshot
	snapshots := clitestutil.ListSnapshotsAndExpectSuccess(t, e2)
	require.Len(t, snapshots, 1)

	// create very small directory
	smallDataDir := filepath.Join(sharedTestDataDirBase, "dir-small")

	testdirtree.CreateDirectoryTree(smallDataDir, testdirtree.DirectoryTreeOptions{
		Depth:                  1,
		MaxSubdirsPerDirectory: 1,
		MaxFilesPerDirectory:   1,
		MaxFileSize:            100,
	}, nil)

	// create snapshot of a very small directory using remote repository client
	e2.RunAndExpectSuccess(t, "snapshot", "create", smallDataDir)

	// make sure snapshot created by the client resulted in blobs being created by the server
	// as opposed to buffering it in memory
	require.Greater(t, len(e.RunAndExpectSuccess(t, "blob", "list", "--prefix=p")), originalPBlobCount)
	require.Greater(t, len(e.RunAndExpectSuccess(t, "blob", "list", "--prefix=q")), originalQBlobCount)

	// create snapshot using remote repository client
	e2.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	// now should see two snapshots
	snapshots = clitestutil.ListSnapshotsAndExpectSuccess(t, e2)
	require.Len(t, snapshots, 3)

	// shutdown the server
	require.NoError(t, serverapi.Shutdown(ctx, controlClient))

	wait2()

	// open repository client to a dead server, this should fail quickly instead of retrying forever.
	timer := timetrack.StartTimer()

	servertesting.ConnectAndOpenAPIServer(t, ctx, &repo.APIServerInfo{
		BaseURL:                             sp.BaseURL,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
	}, repo.ClientOptions{
		Username: "foo",
		Hostname: "bar",
	}, content.CachingOptions{}, "baz", &repo.Options{})

	//nolint:forbidigo
	require.Less(t, timer.Elapsed(), 15*time.Second)
}

func verifyFindManifestCount(ctx context.Context, t *testing.T, rep repo.Repository, pageSize int32, labels map[string]string, wantCount int) {
	t.Helper()

	// use test hook to set requested page size (GRPC only, ignored for legacy API)
	th, ok := rep.(interface {
		SetFindManifestPageSizeForTesting(v int32)
	})
	require.True(t, ok)

	th.SetFindManifestPageSizeForTesting(pageSize)

	man, err := rep.FindManifests(ctx, labels)
	require.NoError(t, err)
	require.Len(t, man, wantCount)
}

func TestFindManifestsPaginationOverGRPC(t *testing.T) {
	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-username", "foo", "--override-hostname", "bar")
	e.RunAndExpectSuccess(t, "server", "users", "add", "foo@bar", "--user-password", "baz")

	tlsCert := filepath.Join(e.ConfigDir, "tls.cert")
	tlsKey := filepath.Join(e.ConfigDir, "tls.key")

	var sp testutil.ServerParameters

	wait, kill := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--grpc",
		"--tls-key-file", tlsKey,
		"--tls-cert-file", tlsCert,
		"--tls-generate-cert",
		"--server-username", uiUsername,
		"--server-password", uiPassword,
		"--server-control-username", controlUsername,
		"--server-control-password", controlPassword)

	defer wait()
	defer kill()

	controlClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            controlUsername,
		Password:                            controlPassword,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)

	waitUntilServerStarted(ctx, t, controlClient)

	rep, err := servertesting.ConnectAndOpenAPIServer(t, ctx, &repo.APIServerInfo{
		BaseURL:                             sp.BaseURL,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
	}, repo.ClientOptions{
		Username: "foo",
		Hostname: "bar",
	}, content.CachingOptions{}, "baz", &repo.Options{})

	require.NoError(t, err)

	defer rep.Close(ctx)

	numManifests := 10000
	uniqueIDs := map[string]struct{}{}

	// add about 36 MB worth of manifests
	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		for range numManifests {
			uniqueID := strings.Repeat(uuid.NewString(), 100)
			require.Len(t, uniqueID, 3600)

			uniqueIDs[uniqueID] = struct{}{}

			if _, err := w.PutManifest(ctx, map[string]string{
				"type":          "snapshot",
				"username":      "foo",
				"hostname":      "bar",
				"verylonglabel": uniqueID,
			}, &snapshot.Manifest{}); err != nil {
				return errors.Wrap(err, "error writing manifest")
			}
		}

		return nil
	}))

	manifests, ferr := rep.FindManifests(ctx, map[string]string{
		"type":     "snapshot",
		"username": "foo",
		"hostname": "bar",
	})

	require.NoError(t, ferr)
	require.Len(t, manifests, numManifests)

	// make sure every manifest is unique and in the uniqueIDs map
	for _, m := range manifests {
		require.Contains(t, uniqueIDs, m.Labels["verylonglabel"])
		delete(uniqueIDs, m.Labels["verylonglabel"])
	}

	// make sure we got them all
	require.Empty(t, uniqueIDs)
}
