//go:build linux
// +build linux

package cli_test

import (
	"net"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestServerControlSocketActivated(t *testing.T) {
	var port int

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	dir0 := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir, "--override-username=another-user", "--override-hostname=another-host")
	env.RunAndExpectSuccess(t, "snap", "create", dir0)

	env.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", env.RepoDir, "--override-username=test-user", "--override-hostname=test-host")

	serverStarted := make(chan struct{})
	serverStopped := make(chan struct{})

	var sp testutil.ServerParameters

	go func() {
		os.Setenv("LISTEN_FDS", "1")
		os.Setenv("LISTEN_PID", strconv.Itoa(os.Getpid()))

		in2, err := syscall.Dup(3)
		if err != nil {
			close(serverStarted)
			return
		}

		defer func() {
			syscall.Close(3)
			syscall.Dup3(in2, 3, 0)
			syscall.Close(in2)
		}()

		syscall.Close(3)

		l1, err := net.Listen("tcp", ":0")
		if err != nil {
			close(serverStarted)
			return
		}

		port = l1.Addr().(*net.TCPAddr).Port

		t.Logf("Activating socket on %v, PID: %v", port, os.Getpid())

		wait, _ := env.RunAndProcessStderr(t, sp.ProcessOutput,
			"server", "start", "--insecure", "--random-server-control-password", "--address=127.0.0.1:0")

		close(serverStarted)
		os.Unsetenv("LISTEN_FDS")
		os.Unsetenv("LISTEN_PID")

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
	require.Contains(t, lines, "REMOTE: another-user@another-host:"+dir0)

	env.RunAndExpectSuccess(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	select {
	case <-serverStopped:
		t.Logf("server shut down")

	case <-time.After(15 * time.Second):
		t.Fatalf("server did not shutdown in time")
	}
}
