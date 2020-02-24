package endtoend_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testlogging"
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

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
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
		"--auto-shutdown=60s",
		"--override-hostname=fake-hostname",
		"--override-username=fake-username",
	)
	t.Logf("detected server parameters %#v", sp)

	cli, err := serverapi.NewClient(serverapi.ClientOptions{
		BaseURL:                             sp.baseURL,
		Password:                            sp.password,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
		LogRequests:                         true,
	})
	if err != nil {
		t.Fatalf("unable to create API client")
	}

	defer cli.Shutdown(ctx) // nolint:errcheck

	waitUntilServerStarted(ctx, t, cli)

	st := verifyServerConnected(t, cli, true)
	if got, want := st.Storage, "filesystem"; got != want {
		t.Errorf("unexpected storage type: %v, want %v", got, want)
	}

	sources := verifySourceCount(t, cli, nil, 1)
	if got, want := sources[0].Source.Path, sharedTestDataDir1; got != want {
		t.Errorf("unexpected source path: %v, want %v", got, want)
	}

	createResp, err := cli.CreateSnapshotSource(ctx, &serverapi.CreateSnapshotSourceRequest{
		Path: sharedTestDataDir2,
	})

	if err != nil {
		t.Fatalf("create snapshot source error: %v", err)
	}

	if !createResp.Created {
		t.Errorf("unexpected value of 'created': %v", createResp.Created)
	}

	if createResp.SnapshotStarted {
		t.Errorf("unexpected value of 'snapshotStarted': %v", createResp.SnapshotStarted)
	}

	verifySourceCount(t, cli, nil, 2)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Host: "no-such-host"}, 0)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	verifySnapshotCount(t, cli, nil, 2)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir1}, 2)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 0)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "no-such-host"}, 0)

	uploadMatchingSnapshots(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2})
	waitForSnapshotCount(ctx, t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	if _, err = cli.CancelUpload(ctx, nil); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	snaps := verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir2}, 1)

	rootPayload, err := cli.GetObject(ctx, snaps[0].RootEntry)
	if err != nil {
		t.Fatalf("getObject %v", err)
	}

	// make sure root payload is valid JSON for the directory.
	var dummy map[string]interface{}
	if err = json.Unmarshal(rootPayload, &dummy); err != nil {
		t.Fatalf("invalid JSON received: %v", err)
	}

	keepDaily := 77

	createResp, err = cli.CreateSnapshotSource(ctx, &serverapi.CreateSnapshotSourceRequest{
		Path:           sharedTestDataDir3,
		CreateSnapshot: true,
		InitialPolicy: policy.Policy{
			RetentionPolicy: policy.RetentionPolicy{
				KeepDaily: &keepDaily,
			},
		},
	})

	if err != nil {
		t.Fatalf("unable to create source")
	}

	if !createResp.SnapshotStarted {
		t.Errorf("unexpected value of 'snapshotStarted': %v", createResp.SnapshotStarted)
	}

	policies, err := cli.ListPolicies(ctx, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir3})
	if err != nil {
		t.Errorf("aaa")
	}

	if len(policies.Policies) != 1 {
		t.Fatalf("unexpected number of policies")
	}

	if got, want := *policies.Policies[0].Policy.RetentionPolicy.KeepDaily, keepDaily; got != want {
		t.Errorf("initial policy not persisted")
	}

	waitForSnapshotCount(ctx, t, cli, &snapshot.SourceInfo{Host: "fake-hostname", UserName: "fake-username", Path: sharedTestDataDir3}, 1)
}

func TestServerStartWithoutInitialRepository(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var sp serverParameters

	connInfo := blob.ConnectionInfo{
		Type: "filesystem",
		Config: filesystem.Options{
			Path: e.RepoDir,
		},
	}

	e.RunAndProcessStderr(t, sp.ProcessOutput, "server", "start", "--ui", "--address=localhost:0", "--random-password", "--tls-generate-cert", "--auto-shutdown=60s")
	t.Logf("detected server parameters %#v", sp)

	cli, err := serverapi.NewClient(serverapi.ClientOptions{
		BaseURL:                             sp.baseURL,
		Password:                            sp.password,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
	})
	if err != nil {
		t.Fatalf("unable to create API client")
	}

	defer cli.Shutdown(ctx) // nolint:errcheck

	waitUntilServerStarted(ctx, t, cli)
	verifyServerConnected(t, cli, false)

	if err = cli.CreateRepository(ctx, &serverapi.CreateRepositoryRequest{
		ConnectRepositoryRequest: serverapi.ConnectRepositoryRequest{
			Password: "foofoo",
			Storage:  connInfo,
		},
	}); err != nil {
		t.Fatalf("create error: %v", err)
	}

	verifyServerConnected(t, cli, true)

	if err = cli.DisconnectFromRepository(ctx); err != nil {
		t.Fatalf("disconnect error: %v", err)
	}

	verifyServerConnected(t, cli, false)

	if err = cli.ConnectToRepository(ctx, &serverapi.ConnectRepositoryRequest{
		Password: "foofoo",
		Storage:  connInfo,
	}); err != nil {
		t.Fatalf("create error: %v", err)
	}

	verifyServerConnected(t, cli, true)
}

func verifyServerConnected(t *testing.T, cli *serverapi.Client, want bool) *serverapi.StatusResponse {
	t.Helper()

	st, err := cli.Status(testlogging.Context(t))
	if err != nil {
		t.Fatalf("status error: %v", err)
	}

	if got := st.Connected; got != want {
		t.Errorf("invalid status connected %v, want %v", st.Connected, want)
	}

	return st
}

func waitForSnapshotCount(ctx context.Context, t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo, want int) {
	t.Helper()

	err := retry.PeriodicallyNoValue(ctx, 1*time.Second, 60, "wait for snapshots", func() error {
		snapshots, err := cli.ListSnapshots(testlogging.Context(t), match)
		if err != nil {
			return errors.Wrap(err, "error listing sources")
		}

		if got := len(snapshots.Snapshots); got != want {
			return errors.Errorf("unexpected number of snapshots %v, want %v", got, want)
		}

		return nil
	}, retry.Always)
	if err != nil {
		t.Fatal(err)
	}
}

func uploadMatchingSnapshots(t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo) {
	t.Helper()

	if _, err := cli.UploadSnapshots(testlogging.Context(t), match); err != nil {
		t.Fatalf("upload failed: %v", err)
	}
}

func verifySnapshotCount(t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo, want int) []*serverapi.Snapshot {
	t.Helper()

	snapshots, err := cli.ListSnapshots(testlogging.Context(t), match)
	if err != nil {
		t.Fatalf("error listing sources: %v", err)
	}

	if got := len(snapshots.Snapshots); got != want {
		t.Errorf("unexpected number of snapshots %v, want %v", got, want)
	}

	return snapshots.Snapshots
}

func verifySourceCount(t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo, want int) []*serverapi.SourceStatus {
	t.Helper()

	sources, err := cli.ListSources(testlogging.Context(t), match)
	if err != nil {
		t.Fatalf("error listing sources: %v", err)
	}

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

func waitUntilServerStarted(ctx context.Context, t *testing.T, cli *serverapi.Client) {
	if err := retry.PeriodicallyNoValue(ctx, 1*time.Second, 60, "wait for server start", func() error {
		_, err := cli.Status(testlogging.Context(t))
		return err
	}, retry.Always); err != nil {
		t.Fatalf("server failed to start")
	}
}
