package endtoend_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/tests/testenv"
)

// Pattern in stderr that `kopia server` uses to pass ephemeral data.
const (
	serverOutputAddress    = "SERVER ADDRESS: "
	serverOutputCertSHA256 = "SERVER CERT SHA256: "
	serverOutputPassword   = "SERVER PASSWORD: "
)

type serverParameters struct {
	baseURL           string
	sha256Fingerprint string
	password          string
}

func (s *serverParameters) ProcessOutput(l string) bool {
	if strings.HasPrefix(l, serverOutputAddress) {
		s.baseURL = strings.TrimPrefix(l, serverOutputAddress)
		return false
	}

	if strings.HasPrefix(l, serverOutputCertSHA256) {
		s.sha256Fingerprint = strings.TrimPrefix(l, serverOutputCertSHA256)
	}

	if strings.HasPrefix(l, serverOutputPassword) {
		s.password = strings.TrimPrefix(l, serverOutputPassword)
	}

	return true
}

func TestServerStart(t *testing.T) {
	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var sp serverParameters

	e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--random-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
		"--override-hostname=fake-hostname",
		"--override-username=fake-username",
		"--ui-title-prefix", "Blah: <script>bleh</script> ",
	)
	t.Logf("detected server parameters %#v", sp)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.baseURL,
		Username:                            "kopia",
		Password:                            sp.password,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, cli)

	waitUntilServerStarted(ctx, t, cli)
	verifyUIServedWithCorrectTitle(t, cli, sp)

	st := verifyServerConnected(t, cli, true)
	require.Equal(t, "filesystem", st.Storage)

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
		Path: sharedTestDataDir2,
	})
	require.NoError(t, err)

	require.True(t, createResp.Created)
	require.False(t, createResp.SnapshotStarted)

	verifySourceCount(t, cli, nil, 2)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Host: "no-such-host"}, 0)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	verifySnapshotCount(t, cli, nil, 2)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir1}, 2)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 0)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "no-such-host"}, 0)

	uploadMatchingSnapshots(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2})
	waitForSnapshotCount(ctx, t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	_, err = serverapi.CancelUpload(ctx, cli, nil)
	require.NoError(t, err)

	snaps := verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	rootPayload, err := serverapi.GetObject(ctx, cli, snaps[0].RootEntry)
	require.NoError(t, err)

	// make sure root payload is valid JSON for the directory.
	var dummy map[string]interface{}
	err = json.Unmarshal(rootPayload, &dummy)
	require.NoError(t, err)

	keepDaily := 77

	createResp, err = serverapi.CreateSnapshotSource(ctx, cli, &serverapi.CreateSnapshotSourceRequest{
		Path:           sharedTestDataDir3,
		CreateSnapshot: true,
		InitialPolicy: policy.Policy{
			RetentionPolicy: policy.RetentionPolicy{
				KeepDaily: &keepDaily,
			},
		},
	})
	require.NoError(t, err)

	require.True(t, createResp.SnapshotStarted)

	policies, err := serverapi.ListPolicies(ctx, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir3})
	require.NoError(t, err)

	require.Len(t, policies.Policies, 1)
	require.Equal(t, keepDaily, *policies.Policies[0].Policy.RetentionPolicy.KeepDaily)

	waitForSnapshotCount(ctx, t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir3}, 1)
}

func TestServerCreateAndConnectViaAPI(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var sp serverParameters

	connInfo := blob.ConnectionInfo{
		Type: "filesystem",
		Config: filesystem.Options{
			Path: e.RepoDir,
		},
	}

	e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start", "--ui",
		"--address=localhost:0", "--random-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation,
	)
	t.Logf("detected server parameters %#v", sp)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.baseURL,
		Username:                            "kopia",
		Password:                            sp.password,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, cli)

	waitUntilServerStarted(ctx, t, cli)
	verifyServerConnected(t, cli, false)

	if err = serverapi.CreateRepository(ctx, cli, &serverapi.CreateRepositoryRequest{
		ConnectRepositoryRequest: serverapi.ConnectRepositoryRequest{
			Password: "foofoo",
			Storage:  connInfo,
		},
	}); err != nil {
		t.Fatalf("create error: %v", err)
	}

	verifyServerConnected(t, cli, true)

	if err = serverapi.DisconnectFromRepository(ctx, cli); err != nil {
		t.Fatalf("disconnect error: %v", err)
	}

	verifyServerConnected(t, cli, false)

	if err = serverapi.ConnectToRepository(ctx, cli, &serverapi.ConnectRepositoryRequest{
		Password: "foofoo",
		Storage:  connInfo,
	}); err != nil {
		t.Fatalf("create error: %v", err)
	}

	verifyServerConnected(t, cli, true)
}

func TestConnectToExistingRepositoryViaAPI(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "repo", "disconnect")

	var sp serverParameters

	connInfo := blob.ConnectionInfo{
		Type: "filesystem",
		Config: filesystem.Options{
			Path: e.RepoDir,
		},
	}

	// at this point repository is not connected, start the server
	e.RunAndProcessStderr(t, sp.ProcessOutput, "server", "start",
		"--ui", "--address=localhost:0", "--random-password",
		"--tls-generate-cert",
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
		"--override-hostname=fake-hostname", "--override-username=fake-username")
	t.Logf("detected server parameters %#v", sp)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.baseURL,
		Username:                            "kopia",
		Password:                            sp.password,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, cli)

	waitUntilServerStarted(ctx, t, cli)
	verifyServerConnected(t, cli, false)

	if err = serverapi.ConnectToRepository(ctx, cli, &serverapi.ConnectRepositoryRequest{
		Password: testenv.TestRepoPassword,
		Storage:  connInfo,
	}); err != nil {
		t.Fatalf("connect error: %v", err)
	}

	verifyServerConnected(t, cli, true)

	si := snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir1}

	uploadMatchingSnapshots(t, cli, &si)

	snaps := waitForSnapshotCount(ctx, t, cli, &si, 3)

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

func TestServerStartInsecure(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=fake-hostname", "--override-username=fake-username")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var sp serverParameters

	// server starts without password and no TLS when --insecure is provided.
	e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--without-password",
		"--insecure",
	)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL: sp.baseURL,
	})
	require.NoError(t, err)

	defer serverapi.Shutdown(ctx, cli)

	waitUntilServerStarted(ctx, t, cli)

	// server fails to start without a password but with TLS.
	e.RunAndExpectFailure(t, "server", "start", "--ui", "--address=localhost:0", "--tls-generate-cert", "--without-password")

	// server fails to start with TLS but without password.
	e.RunAndExpectFailure(t, "server", "start", "--ui", "--address=localhost:0", "--password=foo")
	e.RunAndExpectFailure(t, "server", "start", "--ui", "--address=localhost:0", "--without-password")
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

func waitForSnapshotCount(ctx context.Context, t *testing.T, cli *apiclient.KopiaAPIClient, match *snapshot.SourceInfo, want int) []*serverapi.Snapshot {
	t.Helper()

	var result []*serverapi.Snapshot

	err := retry.PeriodicallyNoValue(ctx, 1*time.Second, 180, "wait for snapshots", func() error {
		snapshots, err := serverapi.ListSnapshots(testlogging.Context(t), cli, match)
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

func verifySnapshotCount(t *testing.T, cli *apiclient.KopiaAPIClient, match *snapshot.SourceInfo, want int) []*serverapi.Snapshot {
	t.Helper()

	snapshots, err := serverapi.ListSnapshots(testlogging.Context(t), cli, match)
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

func verifyUIServedWithCorrectTitle(t *testing.T, cli *apiclient.KopiaAPIClient, sp serverParameters) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), "GET", sp.baseURL, nil)
	require.NoError(t, err)

	req.SetBasicAuth("kopia", sp.password)

	resp, err := cli.HTTPClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	// make sure the UI correctly inserts prefix from KOPIA_UI_TITLE_PREFIX
	// and it's correctly HTML-escaped.
	if !bytes.Contains(b, []byte(`<title>Blah: &lt;script&gt;bleh&lt;/script&gt; Kopia UI`)) {
		t.Fatalf("invalid title served by the UI: %v.", string(b))
	}
}

func waitUntilServerStarted(ctx context.Context, t *testing.T, cli *apiclient.KopiaAPIClient) {
	t.Helper()

	if err := retry.PeriodicallyNoValue(ctx, 1*time.Second, 180, "wait for server start", func() error {
		_, err := serverapi.Status(testlogging.Context(t), cli)
		return err
	}, retry.Always); err != nil {
		t.Fatalf("server failed to start")
	}
}
