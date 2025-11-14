//go:build linux

package socketactivation_test

import (
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestServerControlSocketActivated(t *testing.T) {
	var port int

	serverExe := os.Getenv("KOPIA_SERVER_EXE")
	if serverExe == "" {
		t.Skip("skipping socket-activation test")
	}

	runner := testenv.NewExeRunnerWithBinary(t, serverExe)
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	dir0 := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=another-user", "--override-hostname=another-host")
	env.RunAndExpectSuccess(t, "snap", "create", dir0)

	// The KOPIA_EXE wrapper will set the LISTEN_PID variable for us
	env.Environment["LISTEN_FDS"] = "1"

	l1, err := net.Listen("tcp", ":0")
	require.NoError(t, err, "Failed to open Listener")

	t.Cleanup(func() { l1.Close() })

	port = testutil.EnsureType[*net.TCPAddr](t, l1.Addr()).Port

	t.Logf("Activating socket on port %v", port)

	l1File, err := testutil.EnsureType[*net.TCPListener](t, l1).File()
	require.NoError(t, err, "failed to get filehandle for socket")

	serverStarted := make(chan struct{})
	serverStopped := make(chan struct{})

	var sp testutil.ServerParameters

	go func() {
		runner.ExtraFiles = append(runner.ExtraFiles, l1File)
		wait, _ := env.RunAndProcessStderr(t, sp.ProcessOutput,
			"server", "start", "--insecure", "--random-server-control-password", "--address=127.0.0.1:0")

		l1File.Close()
		close(serverStarted)

		wait()

		close(serverStopped)
	}()

	select {
	case <-serverStarted:
		require.NotEmpty(t, sp.BaseURL, "Failed to start server")
		t.Logf("server started on %v", sp.BaseURL)

	case <-time.After(5 * time.Second):
		t.Fatal("server did not start in time")
	}

	require.Contains(t, sp.BaseURL, ":"+strconv.Itoa(port))

	lines := env.RunAndExpectSuccess(t, "server", "status", "--address", "http://127.0.0.1:"+strconv.Itoa(port), "--server-control-password", sp.ServerControlPassword, "--remote")
	require.Len(t, lines, 1)
	require.Contains(t, lines, "IDLE: another-user@another-host:"+dir0)

	env.RunAndExpectSuccess(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	select {
	case <-serverStopped:
		t.Log("server shut down")

	case <-time.After(15 * time.Second):
		t.Fatal("server did not shutdown in time")
	}
}

func TestServerControlSocketActivatedTooManyFDs(t *testing.T) {
	serverExe := os.Getenv("KOPIA_SERVER_EXE")
	if serverExe == "" {
		t.Skip("skipping socket-activation test")
	}

	runner := testenv.NewExeRunnerWithBinary(t, serverExe)
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=another-user", "--override-hostname=another-host")

	// create 2 file descriptor for a single socket and pass the descriptors to the server
	l1, err := net.Listen("tcp", ":0")
	require.NoError(t, err, "Failed to open Listener")

	t.Cleanup(func() { l1.Close() })

	port := testutil.EnsureType[*net.TCPAddr](t, l1.Addr()).Port

	t.Logf("activation socket port %v", port)

	listener := testutil.EnsureType[*net.TCPListener](t, l1)

	l1File, err := listener.File()
	require.NoError(t, err, "failed to get 1st filehandle for socket")

	t.Cleanup(func() { l1File.Close() })

	l2File, err := listener.File()
	require.NoError(t, err, "failed to get 2nd filehandle for socket")

	t.Cleanup(func() { l2File.Close() })

	runner.ExtraFiles = append(runner.ExtraFiles, l1File, l2File)
	// The KOPIA_EXE wrapper will set the LISTEN_PID variable for us
	env.Environment["LISTEN_FDS"] = "2"

	var gotExpectedErrorMessage atomic.Bool

	stderrAsyncCallback := func(line string) {
		if strings.Contains(line, "Too many activated sockets found.  Expected 1, got 2") {
			gotExpectedErrorMessage.Store(true)
		}
	}

	// although the server is expected to stop quickly with an error, the server's
	// stderr is processed async to avoid test deadlocks if the server continues
	// to run and does not exit.
	wait, kill := env.RunAndProcessStderrAsync(t, func(string) bool { return false }, stderrAsyncCallback, "server", "start", "--insecure", "--random-server-control-password", "--address=127.0.0.1:0")

	t.Cleanup(kill)

	serverStopped := make(chan error)
	go func() {
		defer close(serverStopped)

		serverStopped <- wait()
	}()

	select {
	case err := <-serverStopped:
		require.Error(t, err, "server did not exit with an error")
		t.Log("Done")
	case <-time.After(30 * time.Second):
		t.Fatal("server did not exit in time")
	}

	require.True(t, gotExpectedErrorMessage.Load(), "expected server's stderr to contain a line along the lines of 'Too many activated sockes ...'")
}
