package endtoend_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/snapshot"
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
	t.Parallel()

	ctx := context.Background()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	var sp serverParameters

	e.RunAndProcessStderr(t, sp.ProcessOutput, "server", "start", "--ui", "--address=localhost:0", "--random-password", "--tls-generate-cert", "--auto-shutdown=60s")
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

	waitUntilServerStarted(t, cli)

	st := verifyServerConnected(t, cli, true)
	if got, want := st.Storage, "filesystem"; got != want {
		t.Errorf("unexpected storage type: %v, want %v", got, want)
	}

	sources, err := cli.ListSources(ctx, nil)
	if err != nil {
		t.Fatalf("error listing sources: %v", err)
	}

	if got, want := len(sources.Sources), 1; got != want {
		t.Errorf("unexpected number of sources %v, want %v", got, want)
	}

	if got, want := sources.Sources[0].Source.Path, sharedTestDataDir1; got != want {
		t.Errorf("unexpected source path: %v, want %v", got, want)
	}

	if err = cli.CreateSnapshotSource(ctx, &serverapi.CreateSnapshotSourceRequest{
		Path: sharedTestDataDir2,
	}); err != nil {
		t.Fatalf("create snapshot source error: %v", err)
	}

	verifySourceCount(t, cli, nil, 2)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Host: "no-such-host"}, 0)
	verifySourceCount(t, cli, &snapshot.SourceInfo{Path: sharedTestDataDir2}, 1)

	verifySnapshotCount(t, cli, nil, 2)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Path: sharedTestDataDir1}, 2)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Path: sharedTestDataDir2}, 0)
	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Host: "no-such-host"}, 0)

	uploadMatchingSnapshots(t, cli, &snapshot.SourceInfo{Path: sharedTestDataDir2})
	waitForSnapshotCount(t, cli, &snapshot.SourceInfo{Path: sharedTestDataDir2}, 1)

	if _, err = cli.CancelUpload(ctx, nil); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}

	verifySnapshotCount(t, cli, &snapshot.SourceInfo{Path: sharedTestDataDir2}, 1)
}

func TestServerStartWithoutInitialRepository(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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

	waitUntilServerStarted(t, cli)
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

	st, err := cli.Status(context.Background())
	if err != nil {
		t.Fatalf("status error: %v", err)
	}

	if got := st.Connected; got != want {
		t.Errorf("invalid status connected %v, want %v", st.Connected, want)
	}

	return st
}

func waitForSnapshotCount(t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo, want int) {
	t.Helper()

	err := retry.WithExponentialBackoffNoValue("wait for snapshots", func() error {
		snapshots, err := cli.ListSnapshots(context.Background(), match)
		if err != nil {
			return errors.Wrap(err, "error listing sources")
		}

		if got := len(snapshots.Snapshots); got != want {
			return errors.Errorf("unexpected number of snapshots %v, want %v", got, want)
		}

		return nil
	}, func(err error) bool { return err != nil })
	if err != nil {
		t.Fatal(err)
	}
}

func uploadMatchingSnapshots(t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo) {
	t.Helper()

	if _, err := cli.UploadSnapshots(context.Background(), match); err != nil {
		t.Fatalf("upload failed: %v", err)
	}
}

func verifySnapshotCount(t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo, want int) {
	t.Helper()

	snapshots, err := cli.ListSnapshots(context.Background(), match)
	if err != nil {
		t.Fatalf("error listing sources: %v", err)
	}

	if got := len(snapshots.Snapshots); got != want {
		t.Errorf("unexpected number of snapshots %v, want %v", got, want)
	}
}

func verifySourceCount(t *testing.T, cli *serverapi.Client, match *snapshot.SourceInfo, want int) {
	t.Helper()

	sources, err := cli.ListSources(context.Background(), match)
	if err != nil {
		t.Fatalf("error listing sources: %v", err)
	}

	if got := len(sources.Sources); got != want {
		t.Errorf("unexpected number of sources %v, want %v", got, want)
	}
}

func waitUntilServerStarted(t *testing.T, cli *serverapi.Client) {
	for i := 0; i < 60; i++ {
		if _, err := cli.Status(context.Background()); err == nil {
			return
		}

		time.Sleep(1 * time.Second)
	}

	t.Fatalf("server failed to start")
}
