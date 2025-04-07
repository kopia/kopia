package endtoend_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

const defaultServerControlUsername = "server-control"

func TestServerStart(t *testing.T) {
	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username", "--max-upload-speed=10000000001")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var sp testutil.ServerParameters

	wait, _ := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--random-password",
		"--random-server-control-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
		"--override-hostname=fake-hostname",
		"--override-username=fake-username",
		"--ui-title-prefix", "Blah: <script>bleh</script> ",
	)

	defer wait()

	t.Logf("detected server parameters %#v", sp)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            "kopia",
		Password:                            sp.Password,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	controlClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            defaultServerControlUsername,
		Password:                            sp.ServerControlPassword,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, controlClient)

	waitUntilServerStarted(ctx, t, controlClient)
	verifyUIServedWithCorrectTitle(t, cli, sp)

	verifyServerConnected(t, controlClient, true)
	verifyUIServerConnected(t, cli, true)

	limits, err := serverapi.GetThrottlingLimits(ctx, cli)
	require.NoError(t, err)

	// make sure limits are preserved
	require.Equal(t, 10000000001.0, limits.UploadBytesPerSecond)

	// change the limit via the API.
	limits.UploadBytesPerSecond++
	require.NoError(t, serverapi.SetThrottlingLimits(ctx, cli, limits))

	limits, err = serverapi.GetThrottlingLimits(ctx, cli)
	require.NoError(t, err)
	require.Equal(t, 10000000002.0, limits.UploadBytesPerSecond)

	sources := verifySourceCount(t, cli, nil, 1)
	require.Equal(t, sharedTestDataDir1, sources[0].Source.Path)

	et := estimateSnapshotSize(ctx, t, cli, sharedTestDataDir3)
	require.NotEqual(t, int64(0), et.Counters["Bytes"].Value)
	require.NotEqual(t, int64(0), et.Counters["Directories"].Value)
	require.NotEqual(t, int64(0), et.Counters["Files"].Value)
	require.Equal(t, int64(0), et.Counters["Excluded Directories"].Value)
	require.Equal(t, int64(0), et.Counters["Excluded Files"].Value)
	require.Equal(t, int64(0), et.Counters["Errors"].Value)
	require.Equal(t, int64(0), et.Counters["Ignored Errors"].Value)

	createResp, err := serverapi.CreateSnapshotSource(ctx, cli, &serverapi.CreateSnapshotSourceRequest{
		Path:   sharedTestDataDir2,
		Policy: &policy.Policy{},
	})
	require.NoError(t, err)

	require.False(t, createResp.SnapshotStarted)

	verifySourceCount(t, cli, nil, 2)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Host: "no-such-host"}, 0)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	verifySnapshotCount(t, cli, snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir1}, true, 2)
	verifySnapshotCount(t, cli, snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir1}, false, 1)
	verifySnapshotCount(t, cli, snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, true, 0)
	verifySnapshotCount(t, cli, snapshot.SourceInfo{Host: "no-such-host"}, true, 0)

	uploadMatchingSnapshots(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2})
	waitForSnapshotCount(ctx, t, cli, snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	_, err = serverapi.CancelUpload(ctx, cli, nil)
	require.NoError(t, err)

	snaps := verifySnapshotCount(t, cli, snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, true, 1)

	rootPayload, err := serverapi.GetObject(ctx, cli, snaps[0].RootEntry)
	require.NoError(t, err)

	// make sure root payload is valid JSON for the directory.
	var dummy map[string]interface{}
	err = json.Unmarshal(rootPayload, &dummy)
	require.NoError(t, err)

	keepDaily := policy.OptionalInt(77)

	createResp, err = serverapi.CreateSnapshotSource(ctx, cli, &serverapi.CreateSnapshotSourceRequest{
		Path: sharedTestDataDir3,
		Policy: &policy.Policy{
			RetentionPolicy: policy.RetentionPolicy{
				KeepDaily: &keepDaily,
			},
		},
		CreateSnapshot: true,
	})
	require.NoError(t, err)

	require.True(t, createResp.SnapshotStarted)

	policies, err := serverapi.ListPolicies(ctx, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir3})
	require.NoError(t, err)

	require.Len(t, policies.Policies, 1)
	require.Equal(t, keepDaily, *policies.Policies[0].Policy.RetentionPolicy.KeepDaily)

	waitForSnapshotCount(ctx, t, cli, snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir3}, 1)
}

func TestServerStartAsyncRepoConnect(t *testing.T) {
	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username", "--max-upload-speed=10000000001")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	// now rename the repository directory to simulate unmounting operation
	renamedPath := e.RepoDir + ".renamed"
	require.NoError(t, os.Rename(e.RepoDir, renamedPath))

	var sp testutil.ServerParameters

	e.RunAndExpectFailure(t,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--random-password",
		"--random-server-control-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	// run again - passing --async-repo-connect
	wait, _ := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--random-password",
		"--async-repo-connect",
		"--random-server-control-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	defer wait()

	t.Logf("detected server parameters %#v", sp)

	controlClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            defaultServerControlUsername,
		Password:                            sp.ServerControlPassword,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, controlClient)

	waitUntilServerStarted(ctx, t, controlClient)

	// server is not connected at this point but initialization task is still running.
	sr := verifyServerConnected(t, controlClient, false)
	require.NotEmpty(t, sr.InitRepoTaskID)

	// rename repo dir back
	require.NoError(t, os.Rename(renamedPath, e.RepoDir))

	deadline := clock.Now().Add(30 * time.Second)
	for clock.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)

		st, err := serverapi.Status(ctx, controlClient)
		require.NoError(t, err)

		if st.Connected {
			t.Logf("server connected!")
			break
		}
	}

	require.True(t, clock.Now().Before(deadline), "async connection took too long")
}

func TestServerCreateAndConnectViaAPI(t *testing.T) {
	t.Parallel()

	//nolint:tenv
	os.Setenv("KOPIA_UPGRADE_LOCK_ENABLED", "true")

	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var sp testutil.ServerParameters

	connInfo := blob.ConnectionInfo{
		Type: "filesystem",
		Config: filesystem.Options{
			Path: e.RepoDir,
		},
	}

	wait, _ := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start", "--ui",
		"--address=localhost:0", "--random-password",
		"--random-server-control-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation,
	)

	defer wait()

	t.Logf("detected server parameters %#v", sp)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            "kopia",
		Password:                            sp.Password,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
	})
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	controlClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            defaultServerControlUsername,
		Password:                            sp.ServerControlPassword,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, controlClient)

	waitUntilServerStarted(ctx, t, controlClient)
	verifyServerConnected(t, controlClient, false)
	verifyUIServerConnected(t, cli, false)

	if err = serverapi.CreateRepository(ctx, cli, &serverapi.CreateRepositoryRequest{
		ConnectRepositoryRequest: serverapi.ConnectRepositoryRequest{
			Password: "foofoo",
			Storage:  connInfo,
			ClientOptions: repo.ClientOptions{
				PermissiveCacheLoading: true,
			},
		},
	}); err != nil {
		t.Fatalf("create error: %v", err)
	}

	verifyServerConnected(t, controlClient, true)

	if err = serverapi.DisconnectFromRepository(ctx, cli); err != nil {
		t.Fatalf("disconnect error: %v", err)
	}

	verifyServerConnected(t, controlClient, false)

	if err = serverapi.ConnectToRepository(ctx, cli, &serverapi.ConnectRepositoryRequest{
		Password: "foofoo",
		Storage:  connInfo,
	}); err != nil {
		t.Fatalf("create error: %v", err)
	}

	verifyServerConnected(t, controlClient, true)
}

func TestConnectToExistingRepositoryViaAPI(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "repo", "disconnect")

	var sp testutil.ServerParameters

	connInfo := blob.ConnectionInfo{
		Type: "filesystem",
		Config: filesystem.Options{
			Path: e.RepoDir,
		},
	}

	// at this point repository is not connected, start the server
	wait, _ := e.RunAndProcessStderr(t, sp.ProcessOutput, "server", "start",
		"--ui", "--address=localhost:0", "--random-password",
		"--random-server-control-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
		"--override-hostname=fake-hostname", "--override-username=fake-username",
	)

	defer wait()

	t.Logf("detected server parameters %#v", sp)

	controlClient, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            defaultServerControlUsername,
		Password:                            sp.ServerControlPassword,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, controlClient)

	waitUntilServerStarted(ctx, t, controlClient)
	verifyServerConnected(t, controlClient, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            "kopia",
		Password:                            sp.Password,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
	})
	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	if err = serverapi.ConnectToRepository(ctx, cli, &serverapi.ConnectRepositoryRequest{
		Password: testenv.TestRepoPassword,
		Storage:  connInfo,
	}); err != nil {
		t.Fatalf("connect error: %v", err)
	}

	verifyServerConnected(t, controlClient, true)

	si := snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir1}

	uploadMatchingSnapshots(t, cli, &si)

	snaps := waitForSnapshotCount(ctx, t, cli, si, 3)

	// we're reproducing the bug described in, after connecting to repo via API, next snapshot size becomes zero.
	// https://kopia.discourse.group/t/kopia-0-7-0-not-backing-up-any-files-repro-needed/136/6?u=jkowalski
	minSize := snaps[0].Summary.TotalFileSize
	maxSize := snaps[0].Summary.TotalFileSize

	for _, sn := range snaps {
		v := sn.Summary.TotalFileSize
		if v < minSize {
			minSize = v
		}

		if v > maxSize {
			maxSize = v
		}
	}

	if minSize != maxSize {
		t.Errorf("snapshots don't have consistent size: min %v max %v", minSize, maxSize)
	}
}

func TestServerScheduling(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	emptyDir1 := testutil.TempDirectory(t)
	emptyDir2 := testutil.TempDirectory(t)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username")

	e.RunAndExpectSuccess(t, "snapshot", "create", emptyDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", emptyDir2)
	e.RunAndExpectSuccess(t, "maintenance", "set", "--full-interval", "2s", "--pause-full", "0s")
	e.RunAndExpectSuccess(t, "policy", "set", emptyDir1, "--snapshot-interval=1s")
	e.RunAndExpectSuccess(t, "policy", "set", emptyDir2, "--snapshot-interval=2s")

	var sp testutil.ServerParameters

	// maintenance info before and after server run
	var miBefore, miAfter struct {
		maintenance.Params
		maintenance.Schedule `json:"schedule"`
	}

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &miBefore)

	e.SetLogOutput(true, "server-")

	// start a server, run for 10 seconds and kill it.
	wait, kill := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--insecure",
		"--without-password",
		"--server-control-password=admin-pwd",
	)

	time.Sleep(10 * time.Second)

	kill()
	wait()

	snaps1 := clitestutil.ListSnapshotsAndExpectSuccess(t, e, emptyDir1)[0].Snapshots
	snaps2 := clitestutil.ListSnapshotsAndExpectSuccess(t, e, emptyDir2)[0].Snapshots

	// 10 seconds should be enough to make 8+ snapshots of emptyDir1 and 4+ snapshots of emptyDir2
	require.GreaterOrEqual(t, len(snaps1), 8)
	require.GreaterOrEqual(t, len(snaps2), 4)
	require.Less(t, len(snaps2), len(snaps1))

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &miAfter)

	// make sure we got some maintenance runs
	numRuns := len(miAfter.Schedule.Runs["cleanup-logs"]) - len(miBefore.Schedule.Runs["cleanup-logs"])
	require.Greater(t, numRuns, 2)
	require.Less(t, numRuns, 5)
}

func TestServerStartInsecure(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var sp testutil.ServerParameters

	// server starts without password and no TLS when --insecure is provided.
	wait, _ := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--without-password",
		"--insecure",
	)

	defer wait()

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL: sp.BaseURL,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, cli)

	waitUntilServerStarted(ctx, t, cli)

	// server fails to start with --without-password when `--insecure` is not specified
	e.RunAndExpectFailure(t, "server", "start", "--ui", "--address=localhost:0", "--without-password") // without TLS

	// with TLS
	e.RunAndExpectFailure(t, "server", "start", "--ui",
		"--address=localhost:0",
		"--without-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation,
	)

	// server fails to start when TLS is not configured and `--insecure` is not specified
	e.RunAndExpectFailure(t, "server", "start", "--ui", "--address=localhost:0")
}

func verifyServerConnected(t *testing.T, cli *apiclient.KopiaAPIClient, want bool) *serverapi.StatusResponse {
	t.Helper()

	st, err := serverapi.Status(testlogging.Context(t), cli)
	require.NoError(t, err)

	if got := st.Connected; got != want {
		t.Errorf("invalid status connected %v, want %v", st.Connected, want)
	}

	return st
}

func verifyUIServerConnected(t *testing.T, cli *apiclient.KopiaAPIClient, want bool) *serverapi.StatusResponse {
	t.Helper()

	st, err := serverapi.RepoStatus(testlogging.Context(t), cli)
	require.NoError(t, err)

	if got := st.Connected; got != want {
		t.Errorf("invalid status connected %v, want %v", st.Connected, want)
	}

	return st
}

func waitForSnapshotCount(ctx context.Context, t *testing.T, cli *apiclient.KopiaAPIClient, src snapshot.SourceInfo, want int) []*serverapi.Snapshot {
	t.Helper()

	var result []*serverapi.Snapshot

	err := retry.PeriodicallyNoValue(ctx, 1*time.Second, 180, "wait for snapshots", func() error {
		snapshots, err := serverapi.ListSnapshots(testlogging.Context(t), cli, src, true)
		if err != nil {
			return errors.Wrap(err, "error listing sources")
		}

		if got := len(snapshots.Snapshots); got != want {
			return errors.Errorf("unexpected number of snapshots %v, want %v", got, want)
		}

		result = snapshots.Snapshots

		return nil
	}, retry.Always)

	require.NoError(t, err)

	return result
}

func estimateSnapshotSize(ctx context.Context, t *testing.T, cli *apiclient.KopiaAPIClient, dir string) *uitask.Info {
	t.Helper()

	estimateTask, err := serverapi.Estimate(ctx, cli, &serverapi.EstimateRequest{
		Root:                 dir,
		MaxExamplesPerBucket: 3,
	})
	require.NoError(t, err)

	estimateTaskID := estimateTask.TaskID

	for !estimateTask.Status.IsFinished() {
		time.Sleep(1 * time.Second)

		estimateTask, err = serverapi.GetTask(ctx, cli, estimateTaskID)
		require.NoError(t, err)
	}

	return estimateTask
}

func uploadMatchingSnapshots(t *testing.T, cli *apiclient.KopiaAPIClient, match *snapshot.SourceInfo) {
	t.Helper()

	if _, err := serverapi.UploadSnapshots(testlogging.Context(t), cli, match); err != nil {
		t.Fatalf("upload failed: %v", err)
	}
}

func verifySnapshotCount(t *testing.T, cli *apiclient.KopiaAPIClient, src snapshot.SourceInfo, all bool, want int) []*serverapi.Snapshot {
	t.Helper()

	snapshots, err := serverapi.ListSnapshots(testlogging.Context(t), cli, src, all)
	require.NoError(t, err)

	if got := len(snapshots.Snapshots); got != want {
		t.Errorf("unexpected number of snapshots %v, want %v", got, want)
	}

	return snapshots.Snapshots
}

func verifySourceCount(t *testing.T, cli *apiclient.KopiaAPIClient, match *snapshot.SourceInfo, want int) []*serverapi.SourceStatus {
	t.Helper()

	sources, err := serverapi.ListSources(testlogging.Context(t), cli, match)
	require.NoError(t, err)

	if got, want := sources.LocalHost, "fake-hostname"; got != want {
		t.Errorf("unexpected local host: %v, want %v", got, want)
	}

	if got, want := sources.LocalUsername, "fake-username"; got != want {
		t.Errorf("unexpected local username: %v, want %v", got, want)
	}

	if got := len(sources.Sources); got != want {
		t.Errorf("unexpected number of sources %v, want %v", got, want)
	}

	return sources.Sources
}

func verifyUIServedWithCorrectTitle(t *testing.T, cli *apiclient.KopiaAPIClient, sp testutil.ServerParameters) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, sp.BaseURL, http.NoBody)
	require.NoError(t, err)

	req.SetBasicAuth("kopia", sp.Password)

	resp, err := cli.HTTPClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// make sure the UI correctly inserts prefix from KOPIA_UI_TITLE_PREFIX
	// and it's correctly HTML-escaped.
	if !bytes.Contains(b, []byte(`<title>Blah: &lt;script&gt;bleh&lt;/script&gt; KopiaUI`)) {
		t.Fatalf("invalid title served by the UI: %v.", string(b))
	}
}

func waitUntilServerStarted(ctx context.Context, t *testing.T, cli *apiclient.KopiaAPIClient) {
	t.Helper()

	require.NoError(t, retry.PeriodicallyNoValue(ctx, 1*time.Second, 180, "wait for server start", func() error {
		_, err := serverapi.Status(testlogging.Context(t), cli)
		return err
	}, func(err error) bool {
		var hs apiclient.HTTPStatusError

		if errors.As(err, &hs) {
			switch hs.HTTPStatusCode {
			case http.StatusBadRequest:
				return false
			case http.StatusForbidden:
				return false
			}
		}

		return true
	}))
}
