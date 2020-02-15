package endtoend_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
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

	sources, err := cli.Sources(ctx)
	if err != nil {
		t.Fatalf("error listing sources: %v", err)
	}

	if got, want := len(sources.Sources), 1; got != want {
		t.Errorf("unexpected number of sources %v, want %v", got, want)
	}

	if got, want := sources.Sources[0].Source.Path, sharedTestDataDir1; got != want {
		t.Errorf("unexpected source path: %v, want %v", got, want)
	}
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

	if err = cli.CreateRepository(ctx, &serverapi.CreateRequest{
		ConnectRequest: serverapi.ConnectRequest{
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

	if err = cli.ConnectToRepository(ctx, &serverapi.ConnectRequest{
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

func waitUntilServerStarted(t *testing.T, cli *serverapi.Client) {
	for i := 0; i < 60; i++ {
		if _, err := cli.Status(context.Background()); err == nil {
			return
		}

		time.Sleep(1 * time.Second)
	}

	t.Fatalf("server failed to start")
}
