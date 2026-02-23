package compat_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

var (
	kopiaCurrentExe = os.Getenv("KOPIA_CURRENT_EXE")
	kopia08exe      = os.Getenv("KOPIA_08_EXE")
	kopia017exe     = os.Getenv("KOPIA_017_EXE")
	kopia022exe     = os.Getenv("KOPIA_022_EXE")
)

func TestRepoCreatedWith08CanBeOpenedWithCurrent(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner08 := testenv.NewExeRunnerWithBinary(t, kopia08exe)

	// create repository using v0.8
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "snap", "create", ".")

	// able to open it using current
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runnerCurrent)
	e2.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)
	e2.RunAndExpectSuccess(t, "snap", "ls")

	e2.Environment["KOPIA_UPGRADE_LOCK_ENABLED"] = "1"

	// upgrade
	e2.RunAndExpectSuccess(t, "repository", "upgrade", "begin",
		"--upgrade-owner-id", "owner",
		"--io-drain-timeout", "1s", "--allow-unsafe-upgrade",
		"--status-poll-interval", "1s",
		"--max-permitted-clock-drift", "1s")

	// now 0.8 client can't open it anymore because they won't understand format V2
	e3 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e3.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)

	// old 0.8 client who has cached the format blob and never disconnected
	// can't open the repository because of the poison blob
	e1.RunAndExpectFailure(t, "snap", "ls")
}

func TestRepoCreatedWith08ProperlyRefreshes(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runner08 := testenv.NewExeRunnerWithBinary(t, kopia08exe)

	// create repository using v0.8
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "snap", "create", ".")

	// switch to using latest runner
	e1.Runner = testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)

	// measure time of the cache file and ensure it stays the same
	cachePath := e1.RunAndExpectSuccess(t, "cache", "info", "--path")[0]
	cachedBlob := filepath.Join(cachePath, "kopia.repository")

	time.Sleep(1 * time.Second)
	// 0.12.0 had a bug where we would constantly refresh kopia.repository
	// this was done all the time instead of every 15 minutes,
	st1, err := os.Stat(cachedBlob)
	require.NoError(t, err)

	e1.RunAndExpectSuccess(t, "repo", "status")
	time.Sleep(1 * time.Second)
	e1.RunAndExpectSuccess(t, "repo", "status")

	st2, err := os.Stat(cachedBlob)
	require.NoError(t, err)

	require.Equal(t, st1.ModTime(), st2.ModTime())
}

func TestRepoCreatedWithCurrentWithFormatVersion1CanBeOpenedWith08(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner08 := testenv.NewExeRunnerWithBinary(t, kopia08exe)

	// create repository using current, setting format version to v1
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runnerCurrent)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir, "--format-version=1")
	e1.RunAndExpectSuccess(t, "snap", "create", ".")

	// able to open it using 0.8
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e2.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "snap", "ls")
}

func TestRepoCreatedWithCurrentCannotBeOpenedWith08(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner08 := testenv.NewExeRunnerWithBinary(t, kopia08exe)

	// create repository using current, using default format version (v2)
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runnerCurrent)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "snap", "create", ".")

	// can't to open it using 0.8
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner08)
	e2.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", e1.RepoDir)
}

func TestClientConnectedUsingV017CanConnectUsingCurrent(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	if kopia017exe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner017 := testenv.NewExeRunnerWithBinary(t, kopia017exe)

	// create repository using v0.17 and start a server
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner017)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "server", "users", "add", "foo@bar", "--user-password", "baz")

	var sp testutil.ServerParameters

	tlsCert := filepath.Join(e1.ConfigDir, "tls.cert")
	tlsKey := filepath.Join(e1.ConfigDir, "tls.key")

	wait, kill := e1.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--server-control-username=admin-user",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey,
		"--tls-cert-file", tlsCert,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", sp)

	defer wait()
	defer kill()

	time.Sleep(3 * time.Second)

	// connect to the server using 0.17
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner017)
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

	e2.RunAndExpectSuccess(t, "snapshot", "ls")

	// now switch to using latest executable and old config file,
	// everything should still work
	e2.Runner = runnerCurrent
	e2.RunAndExpectSuccess(t, "snapshot", "ls")
}

// Verify `server status` compatibility for *control* username/password environment variables.
func TestServerControlArgs(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	if kopia022exe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner022 := testenv.NewExeRunnerWithBinary(t, kopia022exe)

	// create repository using v0.22 and start a server
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner022)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "server", "users", "add", "foo@bar", "--user-password", "baz")

	var sp testutil.ServerParameters

	tlsCert := filepath.Join(e1.ConfigDir, "tls.cert")
	tlsKey := filepath.Join(e1.ConfigDir, "tls.key")

	wait, kill := e1.RunAndProcessStderr(t, sp.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--server-control-username=admin-user",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey,
		"--tls-cert-file", tlsCert,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", sp)

	defer wait()
	defer kill()

	time.Sleep(3 * time.Second)

	// check server status using v0.22 environment variables for control username/password
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner022)

	// set v0.22 `server` environment variables
	e2.Environment["KOPIA_SERVER_USERNAME"] = "admin-user"
	e2.Environment["KOPIA_SERVER_PASSWORD"] = "admin-pwd"

	e2.RunAndExpectSuccess(t,
		"server", "status",
		"--address", sp.BaseURL+"/",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
	)

	// now switch to using latest executable with same environment variables,
	// everything should still work
	e2.Runner = runnerCurrent
	e2.RunAndExpectSuccess(t,
		"server", "status",
		"--address", sp.BaseURL+"/",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
	)

	// switch to post-0.22 environment variables,
	// everything should still work
	delete(e2.Environment, "KOPIA_SERVER_USERNAME")
	delete(e2.Environment, "KOPIA_SERVER_PASSWORD")
	e2.Environment["KOPIA_SERVER_CONTROL_USERNAME"] = "admin-user"
	e2.Environment["KOPIA_SERVER_CONTROL_PASSWORD"] = "admin-pwd"

	e2.RunAndExpectSuccess(t,
		"server", "status",
		"--address", sp.BaseURL+"/",
		"--server-cert-fingerprint", sp.SHA256Fingerprint,
	)
}

// Verify `server start` compatibility for *control* username/password arguments and environment variables.
func TestServerStartControlArgs(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	if kopia022exe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)
	runner022 := testenv.NewExeRunnerWithBinary(t, kopia022exe)

	// create repository using v0.22 and start a server
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner022)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "server", "users", "add", "foo@bar", "--user-password", "baz")

	var spExePrevEnvPrev testutil.ServerParameters

	// Use different key files for each `server start` invocation
	// since ServerParameters parses output from TLS key generation
	tlsCert1 := filepath.Join(e1.ConfigDir, "tls1.cert")
	tlsCert2 := filepath.Join(e1.ConfigDir, "tls2.cert")
	tlsCert3 := filepath.Join(e1.ConfigDir, "tls3.cert")
	tlsKey1 := filepath.Join(e1.ConfigDir, "tls1.key")
	tlsKey2 := filepath.Join(e1.ConfigDir, "tls2.key")
	tlsKey3 := filepath.Join(e1.ConfigDir, "tls3.key")

	// set v0.22 `server start` environment variables
	e1.Environment["KOPIA_SERVER_CONTROL_USER"] = "admin-user"
	e1.Environment["KOPIA_SERVER_CONTROL_PASSWORD"] = "admin-pwd"

	wait, killServer := e1.RunAndProcessStderr(t, spExePrevEnvPrev.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey1,
		"--tls-cert-file", tlsCert1,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", spExePrevEnvPrev)

	defer wait()
	// prevent test hang if an assertion fails before reaching `killServer` call
	defer killServer()

	time.Sleep(3 * time.Second)

	// check server status using v0.22 environment variables for control username/password
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner022)

	// set v0.22 `server` environment variables
	e2.Environment["KOPIA_SERVER_USERNAME"] = "admin-user"
	e2.Environment["KOPIA_SERVER_PASSWORD"] = "admin-pwd"

	e2.RunAndExpectSuccess(t,
		"server", "status",
		"--address", spExePrevEnvPrev.BaseURL+"/",
		"--server-cert-fingerprint", spExePrevEnvPrev.SHA256Fingerprint,
	)

	// now switch to using latest executable with same environment variables
	killServer()

	e1.Runner = runnerCurrent

	var spExeNewEnvPrev testutil.ServerParameters

	wait, killServer = e1.RunAndProcessStderr(t, spExeNewEnvPrev.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey2,
		"--tls-cert-file", tlsCert2,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", spExeNewEnvPrev)

	defer wait()
	// prevent test hang if an assertion fails before reaching `killServer` call
	defer killServer()

	time.Sleep(3 * time.Second)

	// check server status using v0.22 environment variables for control username/password
	// everything should still work
	e2.RunAndExpectSuccess(t,
		"server", "status",
		"--address", spExeNewEnvPrev.BaseURL+"/",
		"--server-cert-fingerprint", spExeNewEnvPrev.SHA256Fingerprint,
	)

	// restart server using post-0.22 `server start` environment variables
	killServer()
	delete(e1.Environment, "KOPIA_SERVER_CONTROL_USER")
	delete(e1.Environment, "KOPIA_SERVER_CONTROL_PASSWORD")
	e1.Environment["KOPIA_SERVER_CONTROL_USERNAME"] = "admin-user"
	e1.Environment["KOPIA_SERVER_CONTROL_PASSWORD"] = "admin-pwd"

	var spExeNewEnvNew testutil.ServerParameters

	wait, killServer = e1.RunAndProcessStderr(t, spExeNewEnvNew.ProcessOutput,
		"server", "start",
		"--address=localhost:0",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey3,
		"--tls-cert-file", tlsCert3,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", spExeNewEnvNew)

	defer wait()
	defer killServer()

	time.Sleep(3 * time.Second)

	// check server status using v0.22 environment variables for control username/password
	// everything should still work
	e2.RunAndExpectSuccess(t,
		"server", "status",
		"--address", spExeNewEnvNew.BaseURL+"/",
		"--server-cert-fingerprint", spExeNewEnvNew.SHA256Fingerprint,
	)
}

// Verify `server start` environment variables compatibility for *UI* username/password.
func TestServerStartUIArgs(t *testing.T) {
	t.Parallel()

	if kopiaCurrentExe == "" {
		t.Skip()
	}

	runnerCurrent := testenv.NewExeRunnerWithBinary(t, kopiaCurrentExe)

	// create repository and start a server
	e1 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runnerCurrent)
	e1.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e1.RepoDir)
	e1.RunAndExpectSuccess(t, "server", "users", "add", "foo@bar", "--user-password", "baz")

	// Use different key files for each `server start` invocation
	// since ServerParameters parses output from TLS key generation
	tlsCert1 := filepath.Join(e1.ConfigDir, "tls1.cert")
	tlsCert2 := filepath.Join(e1.ConfigDir, "tls2.cert")
	tlsCert3 := filepath.Join(e1.ConfigDir, "tls3.cert")
	tlsCert4 := filepath.Join(e1.ConfigDir, "tls4.cert")
	tlsKey1 := filepath.Join(e1.ConfigDir, "tls1.key")
	tlsKey2 := filepath.Join(e1.ConfigDir, "tls2.key")
	tlsKey3 := filepath.Join(e1.ConfigDir, "tls3.key")
	tlsKey4 := filepath.Join(e1.ConfigDir, "tls4.key")

	var spArgPrev testutil.ServerParameters

	wait, killServer := e1.RunAndProcessStderr(t, spArgPrev.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--server-control-username=admin-user",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey1,
		"--tls-cert-file", tlsCert1,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
		// set v0.22 `server start` arguments
		"--server-username=ui-user-argPrev",
		"--server-password=ui-pwd-argPrev",
	)

	t.Logf("detected server parameters %#v", spArgPrev)

	defer wait()
	// prevent test hang if an assertion fails before reaching `killServer` call
	defer killServer()

	time.Sleep(3 * time.Second)

	// check server UI connects
	verifyUICredentials(t, spArgPrev, "ui-user-argPrev", "ui-pwd-argPrev")

	// restart server using v0.22 environment variables
	killServer()

	var spEnvPrev testutil.ServerParameters

	// set v0.22 `server start` environment variables
	e1.Environment["KOPIA_SERVER_USERNAME"] = "ui-user-envPrev"
	e1.Environment["KOPIA_SERVER_PASSWORD"] = "ui-pwd-envPrev"

	wait, killServer = e1.RunAndProcessStderr(t, spEnvPrev.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--server-control-username=admin-user",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey2,
		"--tls-cert-file", tlsCert2,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", spEnvPrev)

	defer wait()
	// prevent test hang if an assertion fails before reaching `killServer` call
	defer killServer()

	time.Sleep(3 * time.Second)

	// check server UI connects
	verifyUICredentials(t, spEnvPrev, "ui-user-envPrev", "ui-pwd-envPrev")

	// restart server using post-0.22 `server start` arguments
	killServer()
	delete(e1.Environment, "KOPIA_SERVER_USERNAME")
	delete(e1.Environment, "KOPIA_SERVER_PASSWORD")

	var spArgNew testutil.ServerParameters

	wait, killServer = e1.RunAndProcessStderr(t, spArgNew.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--server-control-username=admin-user",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey3,
		"--tls-cert-file", tlsCert3,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
		// set post-0.22 `server start` arguments
		"--server-ui-username=ui-user-argNew",
		"--server-ui-password=ui-pwd-argNew",
	)

	t.Logf("detected server parameters %#v", spArgNew)

	defer wait()
	// prevent test hang if an assertion fails before reaching `killServer` call
	defer killServer()

	time.Sleep(3 * time.Second)

	// check server UI connects
	verifyUICredentials(t, spArgNew, "ui-user-argNew", "ui-pwd-argNew")

	// restart server using post-0.22 `server start` environment variables
	killServer()

	e1.Environment["KOPIA_SERVER_UI_USERNAME"] = "ui-user-envNew"
	e1.Environment["KOPIA_SERVER_UI_PASSWORD"] = "ui-pwd-envNew"

	var spEnvNew testutil.ServerParameters

	wait, killServer = e1.RunAndProcessStderr(t, spEnvNew.ProcessOutput,
		"server", "start",
		"--ui",
		"--address=localhost:0",
		"--server-control-username=admin-user",
		"--server-control-password=admin-pwd",
		"--tls-generate-cert",
		"--tls-key-file", tlsKey4,
		"--tls-cert-file", tlsCert4,
		"--tls-generate-rsa-key-size=2048", // use shorter key size to speed up generation
	)

	t.Logf("detected server parameters %#v", spEnvNew)

	defer wait()
	defer killServer()

	time.Sleep(3 * time.Second)

	// check server UI connects
	verifyUICredentials(t, spEnvNew, "ui-user-envNew", "ui-pwd-envNew")
}

func verifyUICredentials(t *testing.T, sp testutil.ServerParameters, username, password string) {
	t.Helper()

	require.NotEmpty(t, sp.BaseURL, "ServerParameters must have non-empty BaseURL, check `kopia server start` stderr via KOPIA_TEST_LOG_OUTPUT=1")

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             sp.BaseURL,
		Username:                            username,
		Password:                            password,
		TrustedServerCertificateFingerprint: sp.SHA256Fingerprint,
		LogRequests:                         true,
	})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, sp.BaseURL, http.NoBody)
	require.NoError(t, err)

	resp, err := cli.HTTPClient.Do(req)
	require.NoError(t, err)

	fmt.Println("CLIENT CONNECTED: ", resp)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	fmt.Println("CLIENT READ BODY: ", string(b))

	// make sure the credentials were accepted (e.g. HTTP 200 OK status)
	require.Equal(t, 200, resp.StatusCode, "UI request did not succeed: %v.", string(b))
}
