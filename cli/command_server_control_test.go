package cli_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
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

	// detected from running server
	var (
		serverAddress         string
		serverControlPassword string
	)

	go func() {
		prefix := "SERVER ADDRESS: "
		passwordPrefix := "SERVER CONTROL PASSWORD: "

		kill := env.RunAndProcessStderr(t, func(line string) bool {
			if strings.HasPrefix(line, passwordPrefix) {
				serverControlPassword = strings.TrimPrefix(line, passwordPrefix)
				return true
			}

			if strings.HasPrefix(line, prefix) {
				serverAddress = strings.TrimPrefix(line, prefix)
				close(serverStarted)
				return false
			}

			return true
		}, "server", "start", "--insecure", "--random-server-control-password", "--address=127.0.0.1:0")
		defer kill()

		close(serverStopped)
	}()

	select {
	case <-serverStarted:
		t.Logf("server started on %v", serverAddress)

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not start in time")
	}

	time.Sleep(time.Second)

	lines := env.RunAndExpectSuccess(t, "server", "status", "--address", serverAddress, "--server-control-password", serverControlPassword)
	require.Len(t, lines, 2)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir1)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir2)

	lines = env.RunAndExpectSuccess(t, "server", "status", "--address", serverAddress, "--server-control-password", serverControlPassword, "--remote")
	require.Len(t, lines, 3)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir1)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir2)
	require.Contains(t, lines, "REMOTE: another-user@another-host:"+dir0)

	// create snapshot outside of the server
	env.RunAndExpectSuccess(t, "snap", "create", dir3)
	env.RunAndExpectSuccess(t, "server", "refresh", "--address", serverAddress, "--server-control-password", serverControlPassword)

	lines = env.RunAndExpectSuccess(t, "server", "status", "--address", serverAddress, "--server-control-password", serverControlPassword, "--remote")
	require.Len(t, lines, 4)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir3)

	env.RunAndExpectSuccess(t, "server", "flush", "--address", serverAddress, "--server-control-password", serverControlPassword)

	// trigger server snapshot
	env.RunAndExpectSuccess(t, "server", "snapshot", "--address", serverAddress, "--server-control-password", serverControlPassword, "--all")
	env.RunAndExpectSuccess(t, "server", "snapshot", "--address", serverAddress, "--server-control-password", serverControlPassword, dir1)
	env.RunAndExpectFailure(t, "server", "snapshot", "--address", serverAddress, "--server-control-password", serverControlPassword, "no-such-dir")

	// neither dir nor --all specified
	env.RunAndExpectFailure(t, "server", "snapshot", "--address", serverAddress, "--server-control-password", serverControlPassword)

	// cancel snapshot
	env.RunAndExpectSuccess(t, "server", "cancel", "--address", serverAddress, "--server-control-password", serverControlPassword, "--all")

	env.RunAndExpectSuccess(t, "server", "pause", "--address", serverAddress, "--server-control-password", serverControlPassword, dir1)
	env.RunAndExpectSuccess(t, "server", "resume", "--address", serverAddress, "--server-control-password", serverControlPassword, dir1)

	env.RunAndExpectSuccess(t, "server", "shutdown", "--address", serverAddress, "--server-control-password", serverControlPassword)

	select {
	case <-serverStopped:
		t.Logf("server shut down")

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not shutdown in time")
	}

	// this will fail since the server is down
	env.RunAndExpectFailure(t, "server", "status", "--address", serverAddress, "--server-control-password", serverControlPassword)
	env.RunAndExpectFailure(t, "server", "flush", "--address", serverAddress, "--server-control-password", serverControlPassword)
	env.RunAndExpectFailure(t, "server", "refresh", "--address", serverAddress, "--server-control-password", serverControlPassword)
	env.RunAndExpectFailure(t, "server", "shutdown", "--address", serverAddress, "--server-control-password", serverControlPassword)
}
