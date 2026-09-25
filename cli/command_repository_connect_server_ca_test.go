package cli_test

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

// caClientUsername/caClientPassword is a server-side user account
// registered so the client can open a repository session (distinct from
// --password, which unlocks the repository itself).
const (
	caClientUsername = "server@host"
	caClientPassword = "client-password-for-ca-tests"
)

// newServerEnv creates a filesystem repo owned by server@host.
func newServerEnv(t *testing.T) *testenv.CLITest {
	t.Helper()

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=server", "--override-hostname=host")

	return env
}

// newClientEnv creates a CLI environment with no repo password preset, so
// tests can pass --password explicitly on connect.
func newClientEnv(t *testing.T) *testenv.CLITest {
	t.Helper()

	client := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	delete(client.Environment, "KOPIA_PASSWORD")

	return client
}

// startServerWithLeaf registers caClientUsername and starts a server with
// the given cert/key files, returning its parameters.
func startServerWithLeaf(t *testing.T, e *testenv.CLITest, certFile, keyFile string) testutil.ServerParameters {
	t.Helper()

	e.RunAndExpectSuccess(t, "server", "users", "add", caClientUsername, "--user-password", caClientPassword)

	var sp testutil.ServerParameters

	wait, kill := e.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=127.0.0.1:0",
		"--tls-cert-file", certFile,
		"--tls-key-file", keyFile,
		"--random-server-control-password",
		"--shutdown-grace-period", "100ms",
	)

	t.Cleanup(func() {
		kill()
		wait()
	})

	return sp
}

// fingerprintOf returns the SHA256 fingerprint of cert, hex-encoded, as
// accepted by --server-cert-fingerprint.
func fingerprintOf(cert *x509.Certificate) string {
	h := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(h[:])
}

func writeCAAndLeaf(t *testing.T) (caPath, certFile, keyFile string, leaf *x509.Certificate) {
	t.Helper()

	dir := t.TempDir()

	ca, caKey := testutil.CreateServerRootCA(t)
	leaf, leafKey := testutil.CreateAndSignServerCertificate(t, ca, caKey, "127.0.0.1")

	caPath = testutil.WriteCertPEM(t, dir, "ca.pem", ca)
	certFile = testutil.WriteCertPEM(t, dir, "leaf.pem", leaf)
	keyFile = testutil.WriteKeyPEM(t, dir, "leaf.key", leafKey)

	return caPath, certFile, keyFile, leaf
}

func TestConnectServerCAFile(t *testing.T) {
	caPath, certFile, keyFile, _ := writeCAAndLeaf(t)

	env := newServerEnv(t)

	sp := startServerWithLeaf(t, env, certFile, keyFile)

	client := newClientEnv(t)

	client.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.BaseURL,
		"--server-cert-ca-file", caPath,
		"--override-username=server", "--override-hostname=host",
		"--password", caClientPassword)

	client.RunAndExpectSuccess(t, "snapshot", "list")
}

func TestConnectServerCAFileWrongCA(t *testing.T) {
	_, certFile, keyFile, _ := writeCAAndLeaf(t)
	otherCA, _ := testutil.CreateServerRootCA(t)
	otherCAPath := testutil.WriteCertPEM(t, t.TempDir(), "other-ca.pem", otherCA)

	env := newServerEnv(t)

	sp := startServerWithLeaf(t, env, certFile, keyFile)

	client := newClientEnv(t)

	client.RunAndExpectFailure(t, "repo", "connect", "server",
		"--url", sp.BaseURL,
		"--server-cert-ca-file", otherCAPath,
		"--override-username=server", "--override-hostname=host",
		"--password", caClientPassword)
}

func TestConnectServerFingerprintAndCAFileMutuallyExclusive(t *testing.T) {
	caPath, certFile, keyFile, leaf := writeCAAndLeaf(t)

	env := newServerEnv(t)

	sp := startServerWithLeaf(t, env, certFile, keyFile)

	client := newClientEnv(t)

	_, stderr := client.RunAndExpectFailure(t, "repo", "connect", "server",
		"--url", sp.BaseURL,
		"--server-cert-fingerprint", fingerprintOf(leaf),
		"--server-cert-ca-file", caPath,
		"--override-username=server", "--override-hostname=host",
		"--password", caClientPassword)

	require.Contains(t, joinLines(stderr), "mutually exclusive")
}

func TestServerStatusCAFile(t *testing.T) {
	caPath, certFile, keyFile, _ := writeCAAndLeaf(t)

	env := newServerEnv(t)

	sp := startServerWithLeaf(t, env, certFile, keyFile)

	require.Eventually(t, func() bool {
		_, _, err := env.Run(t, false, "server", "status",
			"--address", sp.BaseURL,
			"--server-cert-ca-file", caPath,
			"--server-control-password", sp.ServerControlPassword)

		return err == nil
	}, 15*time.Second, 100*time.Millisecond)
}

func TestConnectServerCAFileTwoCAsConcatenated(t *testing.T) {
	dir := t.TempDir()

	ca1, _ := testutil.CreateServerRootCA(t)
	ca2, ca2Key := testutil.CreateServerRootCA(t)
	leaf, leafKey := testutil.CreateAndSignServerCertificate(t, ca2, ca2Key, "127.0.0.1")

	bundle := append(testutil.CertPEM(t, ca1), testutil.CertPEM(t, ca2)...)
	bundlePath := filepath.Join(dir, "bundle.pem")
	require.NoError(t, os.WriteFile(bundlePath, bundle, 0o600))

	certFile := testutil.WriteCertPEM(t, dir, "leaf.pem", leaf)
	keyFile := testutil.WriteKeyPEM(t, dir, "leaf.key", leafKey)

	env := newServerEnv(t)

	sp := startServerWithLeaf(t, env, certFile, keyFile)

	client := newClientEnv(t)

	client.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.BaseURL,
		"--server-cert-ca-file", bundlePath,
		"--override-username=server", "--override-hostname=host",
		"--password", caClientPassword)
}

func TestConnectServerCAFileExpiredLeaf(t *testing.T) {
	dir := t.TempDir()

	ca, caKey := testutil.CreateServerRootCA(t)
	leaf, leafKey := testutil.CreateAndSignExpiredServerCertificate(t, ca, caKey, "127.0.0.1")

	caPath := testutil.WriteCertPEM(t, dir, "ca.pem", ca)
	certFile := testutil.WriteCertPEM(t, dir, "leaf.pem", leaf)
	keyFile := testutil.WriteKeyPEM(t, dir, "leaf.key", leafKey)

	env := newServerEnv(t)

	sp := startServerWithLeaf(t, env, certFile, keyFile)

	client := newClientEnv(t)

	client.RunAndExpectFailure(t, "repo", "connect", "server",
		"--url", sp.BaseURL,
		"--server-cert-ca-file", caPath,
		"--override-username=server", "--override-hostname=host",
		"--password", caClientPassword)
}

func TestConnectServerCAFileMissingPath(t *testing.T) {
	env := newServerEnv(t)

	_, stderr := env.RunAndExpectFailure(t, "repo", "connect", "server",
		"--url", "https://127.0.0.1:1",
		"--server-cert-ca-file", "/no/such/ca.pem",
		"--password", caClientPassword)

	require.Contains(t, joinLines(stderr), "error opening server-cert-ca-file")
}

func TestServerCertCAFileEmpty(t *testing.T) {
	env := newServerEnv(t)
	emptyCA := filepath.Join(t.TempDir(), "empty.pem")
	require.NoError(t, os.WriteFile(emptyCA, nil, 0o600))

	_, stderr := env.RunAndExpectFailure(t, "repo", "connect", "server",
		"--url", "https://127.0.0.1:1",
		"--server-cert-ca-file", emptyCA,
		"--password", caClientPassword)
	require.Contains(t, joinLines(stderr), "invalid server-cert-ca-file")

	// server commands validate their flags in a kingpin action, whose error is not written to stderr.
	_, _, err := env.Run(t, true, "server", "status",
		"--address", "https://127.0.0.1:1",
		"--server-cert-ca-file", emptyCA,
		"--server-control-password", "unused")
	require.ErrorContains(t, err, "invalid server-cert-ca-file")
}

func joinLines(lines []string) string {
	return strings.Join(lines, "\n")
}
