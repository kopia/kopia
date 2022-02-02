package cli_test

import (
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

	var sp testutil.ServerParameters

	go func() {
		kill := env.RunAndProcessStderr(t, sp.ProcessOutput,
			"server", "start", "--insecure", "--random-server-control-password", "--address=127.0.0.1:0")

		close(serverStarted)

		defer kill()

		close(serverStopped)
	}()

	select {
	case <-serverStarted:
		t.Logf("server started on %v", sp.BaseURL)

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not start in time")
	}

	time.Sleep(time.Second)

	lines := env.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	require.Len(t, lines, 2)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir1)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir2)

	lines = env.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--remote")
	require.Len(t, lines, 3)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir1)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir2)
	require.Contains(t, lines, "REMOTE: another-user@another-host:"+dir0)

	// create snapshot outside of the server
	env.RunAndExpectSuccess(t, "snap", "create", dir3)
	env.RunAndExpectSuccess(t, "server", "refresh", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	lines = env.RunAndExpectSuccess(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword, "--remote")
	require.Len(t, lines, 4)
	require.Contains(t, lines, "IDLE: test-user@test-host:"+dir3)

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

	env.RunAndExpectSuccess(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)

	select {
	case <-serverStopped:
		t.Logf("server shut down")

	case <-time.After(5 * time.Second):
		t.Fatalf("server did not shutdown in time")
	}

	// this will fail since the server is down
	env.RunAndExpectFailure(t, "server", "status", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	env.RunAndExpectFailure(t, "server", "flush", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	env.RunAndExpectFailure(t, "server", "refresh", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
	env.RunAndExpectFailure(t, "server", "shutdown", "--address", sp.BaseURL, "--server-control-password", sp.ServerControlPassword)
}
