package cli_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestServerUserHashPassword(t *testing.T) {
	const (
		userName = "user78"
		userHost = "client-host"
		userFull = userName + "@" + userHost
	)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-username", "server", "--override-hostname", "host")

	t.Cleanup(func() {
		e.RunAndExpectSuccess(t, "repo", "disconnect")
	})

	userPassword := "bad-password-" + strconv.Itoa(int(rand.Int31()))

	out := e.RunAndExpectSuccess(t, "server", "users", "hash-password", "--user-password", userPassword)

	require.Len(t, out, 1)

	passwordHash := out[0]
	require.NotEmpty(t, passwordHash)

	// attempt to create a user with a bad password hash
	e.RunAndExpectFailure(t, "server", "users", "add", userFull, "--user-password-hash", "bad-base64")

	// create a new user with and set the password using the password hash
	e.RunAndExpectSuccess(t, "server", "users", "add", userFull, "--user-password-hash", passwordHash)

	// start server to test accessing the server with user created above
	var sp testutil.ServerParameters

	wait, kill := e.RunAndProcessStderr(t, sp.ProcessOutput,
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

	// connect to the server repo using a client with the user created above
	cr := testenv.NewInProcRunner(t)
	clientEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, cr)

	delete(clientEnv.Environment, "KOPIA_PASSWORD")

	clientEnv.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.BaseURL,
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
		"--override-username", userName,
		"--override-hostname", userHost,
		"--password", userPassword)

	clientEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	userPassword2 := "bad-password-" + strconv.Itoa(int(rand.Int31()))

	out = e.RunAndExpectSuccess(t, "server", "users", "hash-password", "--user-password", userPassword2)

	require.Len(t, out, 1)

	passwordHash2 := out[0]
	require.NotEmpty(t, passwordHash2)

	// set new user password using the password hash and refresh the server
	e.RunAndExpectSuccess(t, "server", "users", "set", userFull, "--user-password-hash", passwordHash2)
	e.RunAndExpectSuccess(t, "server", "refresh",
		"--address", sp.BaseURL,
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
		"--server-control-password", sp.ServerControlPassword)

	// attempt connecting with the new password
	clientEnv.RunAndExpectSuccess(t, "repo", "connect", "server",
		"--url", sp.BaseURL,
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
		"--override-username", userName,
		"--override-hostname", userHost,
		"--password", userPassword2)

	clientEnv.RunAndExpectSuccess(t, "repo", "disconnect")
}
