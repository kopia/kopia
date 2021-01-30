package endtoend_test

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/tests/testenv"
)

// foo@bar - password baz.
var htpasswdFileContents = []byte("foo@bar:$2y$05$JWrExvBe5Knh0.AMLk5WHu.EzfOP.LhrqMIRf1YseZ/rulBjKqGJ.\n")

func TestAPIServerRepository_GRPC_htpasswd(t *testing.T) {
	t.Parallel()

	testAPIServerRepository(t, []string{"--no-legacy-api"}, true, false)
}

func TestAPIServerRepository_GRPC_RepositoryUsers(t *testing.T) {
	t.Parallel()

	testAPIServerRepository(t, []string{"--no-legacy-api"}, true, true)
}

func TestAPIServerRepository_DisableGRPC_htpasswd(t *testing.T) {
	t.Parallel()

	testAPIServerRepository(t, []string{"--no-grpc"}, false, false)
}

// nolint:thelper
func testAPIServerRepository(t *testing.T, serverStartArgs []string, useGRPC, allowRepositoryUsers bool) {
	ctx := testlogging.Context(t)

	var connectArgs []string

	if !useGRPC {
		connectArgs = []string{"--no-grpc"}
	}

	e := testenv.NewCLITest(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create one snapshot as foo@bar
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-username", "foo", "--override-hostname", "bar")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e1 := testenv.NewCLITest(t)
	defer e1.RunAndExpectSuccess(t, "repo", "disconnect")

	// create one snapshot as not-foo@bar
	e1.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir, "--override-username", "not-foo", "--override-hostname", "bar")
	e1.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	originalPBlobCount := len(e1.RunAndExpectSuccess(t, "blob", "list", "--prefix=p"))
	originalQBlobCount := len(e1.RunAndExpectSuccess(t, "blob", "list", "--prefix=q"))

	tlsCert := filepath.Join(e.ConfigDir, "tls.cert")
	tlsKey := filepath.Join(e.ConfigDir, "tls.key")

	if allowRepositoryUsers {
		e.RunAndExpectSuccess(t, "users", "add", "foo@bar", "--user-password", "baz")

		serverStartArgs = append(serverStartArgs, "--allow-repository-users")
	} else {
		htpasswordFile := filepath.Join(e.ConfigDir, "htpasswd.txt")
		ioutil.WriteFile(htpasswordFile, htpasswdFileContents, 0o755)
		serverStartArgs = append(serverStartArgs, "--htpasswd-file", htpasswordFile)
	}

	var sp serverParameters

	e.RunAndProcessStderr(t, sp.ProcessOutput,
		append([]string{
			"server", "start",
			"--address=localhost:0",
			"--tls-generate-cert",
			"--tls-key-file", tlsKey,
			"--tls-cert-file", tlsCert,
			"--auto-shutdown=60s",
		}, serverStartArgs...)...)
	t.Logf("detected server parameters %#v", sp)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.baseURL,
		Username:                            "foo@bar",
		Password:                            "baz",
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
		LogRequests:                         true,
	})
	if err != nil {
		t.Fatalf("unable to create API apiclient")
	}

	waitUntilServerStarted(ctx, t, cli)

	// open repository client.
	rep, err := repo.OpenAPIServer(ctx, &repo.APIServerInfo{
		BaseURL:                             sp.baseURL,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
		DisableGRPC:                         !useGRPC,
	}, repo.ClientOptions{
		Username: "foo",
		Hostname: "bar",
	}, "baz")
	if err != nil {
		t.Fatal(err)
	}

	// open new write session in repository client

	writeSess, err := rep.NewWriter(ctx, "some writer")
	if err != nil {
		t.Fatal(err)
	}

	serverapi.Shutdown(ctx, cli)

	// give the server a moment to wind down.
	time.Sleep(1 * time.Second)

	defer rep.Close(ctx)

	// start the server again, using the same address & TLS key+cert, so existing connection
	// should be re-established.
	e.RunAndProcessStderr(t, sp.ProcessOutput,
		append([]string{
			"server", "start",
			"--address=" + sp.baseURL,
			"--tls-key-file", tlsKey,
			"--tls-cert-file", tlsCert,
			"--auto-shutdown=60s",
		}, serverStartArgs...)...)
	t.Logf("detected server parameters %#v", sp)

	cli, err = apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.baseURL,
		Username:                            "foo@bar",
		Password:                            "baz",
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
		LogRequests:                         true,
	})
	if err != nil {
		t.Fatalf("unable to create API apiclient")
	}

	waitUntilServerStarted(ctx, t, cli)

	defer serverapi.Shutdown(ctx, cli)

	someLabels := map[string]string{
		"type":     "snapshot",
		"username": "foo",
		"hostname": "bar",
	}

	// invoke some read method, the repository will automatically reconnect to the server.
	verifyFindManifestCount(ctx, t, rep, someLabels, 1)

	if useGRPC {
		// the same method on a GRPC write session should fail because the stream was broken.
		if _, err := writeSess.FindManifests(ctx, someLabels); err == nil {
			t.Fatalf("expected failure on write session method, got success.")
		}
	} else {
		// invoke some method on write session, this will succeed because legacy API is stateless
		// (also incorrect in this case).
		verifyFindManifestCount(ctx, t, writeSess, someLabels, 1)
	}

	e2 := testenv.NewCLITest(t)
	defer e2.RunAndExpectSuccess(t, "repo", "disconnect")

	e2.RunAndExpectSuccess(t, append([]string{
		"repo", "connect", "server",
		"--url", sp.baseURL + "/",
		"--server-cert-fingerprint", sp.sha256Fingerprint,
		"--override-username", "foo",
		"--override-hostname", "bar",
		"--password", "baz",
	}, connectArgs...)...)

	// we are providing custom password to connect, make sure we won't be providing
	// (different) default password via environment variable, as command-line password
	// takes precedence over persisted password.
	e2.RemoveDefaultPassword()

	// should see one snapshot
	snapshots := e2.ListSnapshotsAndExpectSuccess(t)
	if got, want := len(snapshots), 1; got != want {
		t.Errorf("invalid number of snapshots for foo@bar")
	}

	// create very small directory
	smallDataDir := filepath.Join(sharedTestDataDirBase, "dir-small")

	testenv.CreateDirectoryTree(smallDataDir, testenv.DirectoryTreeOptions{
		Depth:                  1,
		MaxSubdirsPerDirectory: 1,
		MaxFilesPerDirectory:   1,
		MaxFileSize:            100,
	}, nil)

	// create snapshot of a very small directory using remote repository client
	e2.RunAndExpectSuccess(t, "snapshot", "create", smallDataDir)

	// make sure snapshot created by the client resulted in blobs being created by the server
	// as opposed to buffering it in memory
	if got, want := len(e.RunAndExpectSuccess(t, "blob", "list", "--prefix=p")), originalPBlobCount; got <= want {
		t.Errorf("unexpected number of P blobs on the server: %v, wanted > %v", got, want)
	}

	if got, want := len(e.RunAndExpectSuccess(t, "blob", "list", "--prefix=q")), originalQBlobCount; got <= want {
		t.Errorf("unexpected number of Q blobs on the server: %v, wanted > %v", got, want)
	}

	// create snapshot using remote repository client
	e2.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	// now should see two snapshots
	snapshots = e2.ListSnapshotsAndExpectSuccess(t)
	if got, want := len(snapshots), 3; got != want {
		t.Errorf("invalid number of snapshots for foo@bar")
	}

	// shutdown the server
	serverapi.Shutdown(ctx, cli)

	// open repository client to a dead server, this should fail quickly instead of retrying forever.
	t0 := time.Now()

	repo.OpenAPIServer(ctx, &repo.APIServerInfo{
		BaseURL:                             sp.baseURL,
		TrustedServerCertificateFingerprint: sp.sha256Fingerprint,
		DisableGRPC:                         !useGRPC,
	}, repo.ClientOptions{
		Username: "foo",
		Hostname: "bar",
	}, "baz")

	if dur := time.Since(t0); dur > 15*time.Second {
		t.Fatalf("failed connection took %v", dur)
	}
}

func verifyFindManifestCount(ctx context.Context, t *testing.T, rep repo.Repository, labels map[string]string, wantCount int) {
	t.Helper()

	man, err := rep.FindManifests(ctx, labels)
	if err != nil {
		t.Fatalf("unable to list manifests using repository %v", err)
	}

	if got, want := len(man), wantCount; got != want {
		t.Fatalf("invalid number of manifests: %v, want %v", got, want)
	}
}
