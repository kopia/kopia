package cli_test

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestServerTLS(t *testing.T) {
	const (
		userName = "user78"
		userHost = "client-host"
		userFull = userName + "@" + userHost
	)

	randomN, _ := rand.Int(rand.Reader, big.NewInt(1000))
	userPassword := fmt.Sprintf("password-%v", randomN.Int64())

	serverActiveCA, err := testutil.CreateRootCA("kopia-active", "server-tls-test")
	require.NoError(t, err, "creating server (active) TLS CA certificate")

	serverInactiveCA, err := testutil.CreateRootCA("kopia-inactive", "server-tls-test")
	require.NoError(t, err, "creating server (inactive) TLS CA certificate")

	clientActive, err := serverActiveCA.CreateAndSignClientCertificate(userFull)
	require.NoError(t, err, "creating client (active) TLS certificate")

	clientExpired, err := serverActiveCA.CreateAndSignExpiredClientCertificate(userFull)
	require.NoError(t, err, "creating client (active) TLS certificate")

	clientInactive, err := serverInactiveCA.CreateAndSignClientCertificate(userFull)
	require.NoError(t, err, "creating client (inactive) TLS certificate")

	certDir := testutil.TempDirectory(t)

	err = serverActiveCA.WriteTo(
		path.Join(certDir, "kopia-active-ca.crt"),
		path.Join(certDir, "kopia-active-ca.key"),
	)
	require.NoError(t, err, "writing server (active) TLS certificate")

	err = serverInactiveCA.WriteTo(
		path.Join(certDir, "kopia-inactive-ca.crt"),
		path.Join(certDir, "kopia-inactive-ca.key"),
	)
	require.NoError(t, err, "writing server (inactive) TLS certificate")

	err = clientActive.WriteTo(
		path.Join(certDir, "client-active.crt"),
		path.Join(certDir, "client-active.key"),
	)
	require.NoError(t, err, "writing client (active) TLS certificate")

	err = clientExpired.WriteTo(
		path.Join(certDir, "client-expired.crt"),
		path.Join(certDir, "client-expired.key"),
	)
	require.NoError(t, err, "writing client (expired) TLS certificate")

	err = clientInactive.WriteTo(
		path.Join(certDir, "client-inactive.crt"),
		path.Join(certDir, "client-inactive.key"),
	)
	require.NoError(t, err, "writing client (inactive) TLS certificate")

	t.Run("no tls-ca-file", func(t *testing.T) {
		clientEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
		delete(clientEnv.Environment, "KOPIA_PASSWORD")

		serverEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
		defer serverEnv.RunAndExpectSuccess(t, "repo", "disconnect")

		serverEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", serverEnv.RepoDir)
		serverEnv.RunAndExpectSuccess(t, "server", "users", "add", userFull, "--user-password", userPassword)

		var sp testutil.ServerParameters

		wait, kill := serverEnv.RunAndProcessStderr(t, sp.ProcessOutput,
			"server", "start",
			"--address=localhost:0",
			"--tls-generate-cert",
			"--random-server-control-password",
			"--shutdown-grace-period", "100ms",
		)
		t.Cleanup(func() {
			kill()
			wait()
			t.Log("server stopped")
		})

		t.Logf("detected server parameters %#v", sp)

		t.Run("no client cert, good password", func(t *testing.T) {
			clientEnv.RunAndExpectSuccess(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--override-username", userName,
				"--override-hostname", userHost,
				"--password", userPassword)
		})
		t.Run("no client cert, bad password", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--override-username", userName,
				"--override-hostname", userHost,
				"--password", "bad-"+userPassword)
		})
		t.Run("valid client cert, ignored", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-active.crt"),
				"--client-key", path.Join(certDir, "client-active.key"),
				"--override-username", userName,
				"--override-hostname", userHost)
		})
	})

	t.Run("with-tls-ca-file", func(t *testing.T) {
		clientEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
		delete(clientEnv.Environment, "KOPIA_PASSWORD")

		serverEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
		defer serverEnv.RunAndExpectSuccess(t, "repo", "disconnect")

		serverEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", serverEnv.RepoDir)
		serverEnv.RunAndExpectSuccess(t, "server", "users", "add", userFull, "--user-password", userPassword)

		var sp testutil.ServerParameters

		wait, kill := serverEnv.RunAndProcessStderr(t, sp.ProcessOutput,
			"server", "start",
			"--address=localhost:0",
			"--tls-generate-cert",
			"--random-server-control-password",
			"--shutdown-grace-period", "100ms",
			"--tls-ca-file="+path.Join(certDir, "kopia-active-ca.crt"),
		)
		t.Cleanup(func() {
			kill()
			wait()
			t.Log("server stopped")
		})

		t.Logf("detected server parameters %#v", sp)

		t.Run("no client cert, good password", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--override-username", userName,
				"--override-hostname", userHost,
				"--password", userPassword)
		})
		t.Run("no client cert, bad password", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--override-username", userName,
				"--override-hostname", userHost,
				"--password", "bad-"+userPassword)
		})
		t.Run("valid client cert", func(t *testing.T) {
			clientEnv.RunAndExpectSuccess(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-active.crt"),
				"--client-key", path.Join(certDir, "client-active.key"),
				"--override-username", userName,
				"--override-hostname", userHost)
		})
		t.Run("valid client cert, but no private key has been specified", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-active.crt"),
				"--override-username", userName,
				"--override-hostname", userHost)
		})
		t.Run("valid client cert, but no public key has been specified", func(t *testing.T) {
			clientEnv.RunAndExpectSuccess(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-active.crt"),
				"--client-key", path.Join(certDir, "client-active.key"),
				"--override-username", userName,
				"--override-hostname", userHost)
		})
		t.Run("valid client cert, username not coherent", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-active.crt"),
				"--client-key", path.Join(certDir, "client-active.key"),
				"--override-username", "bad-"+userName,
				"--override-hostname", userHost)
		})
		t.Run("valid client cert, hostname not coherent", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-active.crt"),
				"--client-key", path.Join(certDir, "client-active.key"),
				"--override-username", userName,
				"--override-hostname", "bad-"+userHost)
		})
		t.Run("invalid client cert (wrong CA)", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-inactive.crt"),
				"--client-key", path.Join(certDir, "client-inactive.key"),
				"--override-username", userName,
				"--override-hostname", userHost)
		})
		t.Run("invalid client cert (expired)", func(t *testing.T) {
			clientEnv.RunAndExpectFailure(t, "repo", "connect", "server",
				"--url", sp.BaseURL,
				"--server-cert-fingerprint", sp.SHA256Fingerprint,
				"--client-certificate", path.Join(certDir, "client-expired.crt"),
				"--client-key", path.Join(certDir, "client-expired.key"),
				"--override-username", userName,
				"--override-hostname", userHost)
		})
	})
}
