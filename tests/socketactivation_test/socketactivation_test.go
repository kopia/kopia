//go:build linux
// +build linux

package socketactivation_test

import (
	"net"
	"os"
	"strconv"
	"strings"
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
	if err != nil {
		t.Fatalf("Failed to open Listener")
	}

	defer func() {
		l1.Close()
	}()

	port = l1.Addr().(*net.TCPAddr).Port

	t.Logf("Activating socket on port %v", port)

	serverStarted := make(chan struct{})
	serverStopped := make(chan struct{})

	var sp testutil.ServerParameters

	go func() {
		l1File, err := l1.(*net.TCPListener).File()
		if err != nil {
			t.Logf("ERROR: Failed to get filehandle for socket")
			close(serverStarted)

			return
		}

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
		if sp.BaseURL == "" {
			t.Fatalf("Failed to start server")
		}

		t.Logf("server started on %v", sp.BaseURL)

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not start in time")
	}

	require.Contains(t, sp.BaseURL, ":"+strconv.Itoa(port))

	lines := env.RunAndExpectSuccess(t, "server", "status", "--address", "http://127.0.0.1:"+strconv.Itoa(port), "--server-control-password", sp.ServerControlPassword, "--remote")
	require.Len(t, lines, 1)
	require.Contains(t, lines, "IDLE: another-user@another-host:"+dir0)

	env.RunAndExpectSuccess(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	select {
	case <-serverStopped:
		t.Logf("server shut down")

	case <-time.After(15 * time.Second):
		t.Fatalf("server did not shutdown in time")
	}
}

func TestServerControlSocketActivatedTooManyFDs(t *testing.T) {
	var port int

	serverExe := os.Getenv("KOPIA_SERVER_EXE")
	if serverExe == "" {
		t.Skip("skipping socket-activation test")
	}

	runner := testenv.NewExeRunnerWithBinary(t, serverExe)
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=another-user", "--override-hostname=another-host")
	// The KOPIA_EXE wrapper will set the LISTEN_PID variable for us
	env.Environment["LISTEN_FDS"] = "2"

	l1, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to open Listener")
	}

	defer func() {
		l1.Close()
	}()

	port = l1.Addr().(*net.TCPAddr).Port

	t.Logf("Activating socket on port %v", port)

	serverStarted := make(chan []string)

	go func() {
		l1File, err := l1.(*net.TCPListener).File()
		if err != nil {
			t.Logf("Failed to get filehandle for socket")
			close(serverStarted)

			return
		}

		l2File, err := l1.(*net.TCPListener).File()
		if err != nil {
			t.Logf("Failed to get 2nd filehandle for socket")
			close(serverStarted)

			return
		}

		runner.ExtraFiles = append(runner.ExtraFiles, l1File, l2File)

		_, stderr := env.RunAndExpectFailure(t, "server", "start", "--insecure", "--random-server-control-password", "--address=127.0.0.1:0")

		l1File.Close()
		l2File.Close()
		serverStarted <- stderr
		close(serverStarted)
	}()

	select {
	case stderr := <-serverStarted:
		require.Contains(t, strings.Join(stderr, ""), "Too many activated sockets found.  Expected 1, got 2")
		t.Logf("Done")

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not exit in time")
	}
}
