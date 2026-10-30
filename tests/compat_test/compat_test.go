package compat_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

var (
	kopiaCurrentExe = os.Getenv("KOPIA_CURRENT_EXE")
	kopia08exe      = os.Getenv("KOPIA_08_EXE")
	kopia017exe     = os.Getenv("KOPIA_017_EXE")
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
