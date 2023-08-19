package cli_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/kopia/kopia/tests/testenv"
)

func TestServerControl(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	dir0 := testutil.TempDirectory(t)
	dir1 := testutil.TempDirectory(t)
	dir2 := testutil.TempDirectory(t)
	dir3 := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=another-user", "--override-hostname=another-host")
	env.RunAndExpectSuccess(t, "snap", "create", dir0)

	env.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", env.RepoDir, "--override-username=test-user", "--override-hostname=test-host")
	env.RunAndExpectSuccess(t, "snap", "create", dir1)
	env.RunAndExpectSuccess(t, "snap", "create", dir2)

	serverStarted := make(chan struct{})
	serverStopped := make(chan struct{})

	var sp testutil.ServerParameters

	go func() {
		wait, _ := env.RunAndProcessStderr(t, sp.ProcessOutput,
			"server", "start", "--insecure", "--random-server-control-password", "--address=127.0.0.1:0")

		close(serverStarted)

		wait()

		close(serverStopped)
	}()

	select {
	case <-serverStarted:
		t.Logf("server started on %v", sp.BaseURL)

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not start in time")
	}

	const (
		pollFrequency = 100 * time.Millisecond
		waitTimeout   = 15 * time.Second
	)

	require.Eventually(t, func() bool {
		lines := env.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
		t.Logf("lines: %v", lines)
		return hasLine(lines, "IDLE: test-user@test-host:"+dir1) && hasLine(lines, "IDLE: test-user@test-host:"+dir2)
	}, waitTimeout, pollFrequency)

	lines := env.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--remote")
	require.Len(t, lines, 3)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir1)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir2)
	require.Contains(t, lines, "REMOTE: another-user@another-host:"+dir0)

	// create snapshot outside of the server
	env.RunAndExpectSuccess(t, "snap", "create", dir3)
	env.RunAndExpectSuccess(t, "server", "refresh", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	require.Eventually(t, func() bool {
		lines := env.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--remote")
		t.Logf("lines: %v", lines)
		return hasLine(lines, "IDLE: test-user@test-host:"+dir3)
	}, waitTimeout, pollFrequency)

	env.RunAndExpectSuccess(t, "server", "flush", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	// trigger server snapshot
	env.RunAndExpectSuccess(t, "server", "snapshot", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--all")
	env.RunAndExpectSuccess(t, "server", "snapshot", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, dir1)
	env.RunAndExpectFailure(t, "server", "snapshot", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "no-such-dir")

	// neither dir nor --all specified
	env.RunAndExpectFailure(t, "server", "snapshot", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	// cancel snapshot
	env.RunAndExpectSuccess(t, "server", "cancel", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--all")

	env.RunAndExpectSuccess(t, "server", "pause", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, dir1)
	env.RunAndExpectSuccess(t, "server", "resume", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, dir1)

	env.RunAndExpectSuccess(t, "server", "throttle", "set", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword,
		"--download-bytes-per-second=1000000000",
		"--upload-bytes-per-second=2000000000",
		"--read-requests-per-second=300",
		"--write-requests-per-second=400",
		"--list-requests-per-second=500",
		"--concurrent-reads=300",
		"--concurrent-writes=400",
	)

	require.Equal(t, []string{
		"Max Download Speed:            1 GB/s",
		"Max Upload Speed:              2 GB/s",
		"Max Read Requests Per Second:  300",
		"Max Write Requests Per Second: 400",
		"Max List Requests Per Second:  500",
		"Max Concurrent Reads:          300",
		"Max Concurrent Writes:         400",
	}, env.RunAndExpectSuccess(t, "server", "throttle", "get", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword))

	env.RunAndExpectSuccess(t, "server", "throttle", "set", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword,
		"--upload-bytes-per-second=unlimited",
		"--write-requests-per-second=unlimited",
	)

	env.RunAndExpectFailure(t, "server", "throttle", "set", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword,
		"--upload-bytes-per-second=-10",
	)

	require.Equal(t, []string{
		"Max Download Speed:            1 GB/s",
		"Max Upload Speed:              (unlimited)",
		"Max Read Requests Per Second:  300",
		"Max Write Requests Per Second: (unlimited)",
		"Max List Requests Per Second:  500",
		"Max Concurrent Reads:          300",
		"Max Concurrent Writes:         400",
	}, env.RunAndExpectSuccess(t, "server", "throttle", "get", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword))

	var limits throttling.Limits

	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "server", "throttle", "get", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--json"), &limits)
	require.Equal(t, throttling.Limits{
		ReadsPerSecond:         300,
		WritesPerSecond:        0,
		ListsPerSecond:         500,
		UploadBytesPerSecond:   0,
		DownloadBytesPerSecond: 1e+09,
		ConcurrentReads:        300,
		ConcurrentWrites:       400,
	}, limits)

	env.RunAndExpectSuccess(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	select {
	case <-serverStopped:
		t.Logf("server shut down")

	case <-time.After(15 * time.Second):
		t.Fatalf("server did not shutdown in time")
	}

	// this will fail since the server is down
	env.RunAndExpectFailure(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	env.RunAndExpectFailure(t, "server", "flush", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	env.RunAndExpectFailure(t, "server", "refresh", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	env.RunAndExpectFailure(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
}

func TestServerControlUDS(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	dir0 := testutil.TempDirectory(t)
	// the socket path must be < 108 bytes (linux) or 104 bytes (Mac), so can't use long tempdir
	dir1 := testutil.TempDirectoryShort(t)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=another-user", "--override-hostname=another-host")
	env.RunAndExpectSuccess(t, "snap", "create", dir0)

	env.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", env.RepoDir, "--override-username=test-user", "--override-hostname=test-host")

	serverStarted := make(chan struct{})
	serverStopped := make(chan struct{})

	var sp testutil.ServerParameters

	go func() {
		wait, _ := env.RunAndProcessStderr(t, sp.ProcessOutput,
			"server", "start", "--insecure", "--random-server-control-password", "--address="+"unix:"+dir1+"/sock")

		close(serverStarted)

		wait()

		close(serverStopped)
	}()

	select {
	case <-serverStarted:
		t.Logf("server started on %v", sp.BaseURL)

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not start in time")
	}

	lines := env.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--remote")
	require.Len(t, lines, 1)
	require.Contains(t, lines, "REMOTE: another-user@another-host:"+dir0)

	env.RunAndExpectSuccess(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	select {
	case <-serverStopped:
		t.Logf("server shut down")

	case <-time.After(15 * time.Second):
		t.Fatalf("server did not shutdown in time")
	}
}

func hasLine(lines []string, lookFor string) bool {
	for _, l := range lines {
		if l == lookFor {
			return true
		}
	}

	return false
}
