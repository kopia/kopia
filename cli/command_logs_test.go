package cli_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestLogsCommands(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// one log from repository creation
	e.RunAndVerifyOutputLineCount(t, 1, "logs", "list")

	// verify we did not add a log
	e.RunAndVerifyOutputLineCount(t, 1, "logs", "list")

	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))

	e.RunAndVerifyOutputLineCount(t, 2, "logs", "list")

	// sleep a bit so that 3rd log is reliably last in time order even if the clock is completely
	// messed up.
	time.Sleep(2 * time.Second)
	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))

	lines := e.RunAndVerifyOutputLineCount(t, 3, "logs", "list")
	firstLogID := strings.Split(lines[0], " ")[0]
	secondLogID := strings.Split(lines[1], " ")[0]
	thirdLogID := strings.Split(lines[2], " ")[0]

	firstLogLines := e.RunAndExpectSuccess(t, "logs", "show", firstLogID)
	secondLogLines := e.RunAndExpectSuccess(t, "logs", "show", secondLogID)
	thirdLogLines := e.RunAndExpectSuccess(t, "logs", "show", thirdLogID)
	e.RunAndExpectFailure(t, "logs", "show", "no-such-log")

	require.NotEqual(t, firstLogLines, secondLogLines)
	require.NotEqual(t, secondLogLines, thirdLogLines)

	// by default cleanup retains a lot of logs.
	e.RunAndExpectSuccess(t, "logs", "cleanup")
	e.RunAndVerifyOutputLineCount(t, 3, "logs", "list")
	e.RunAndExpectSuccess(t, "logs", "cleanup", "--max-count=2")
	e.RunAndVerifyOutputLineCount(t, 2, "logs", "list")
	e.RunAndExpectSuccess(t, "logs", "cleanup", "--max-count=1")
	e.RunAndVerifyOutputLineCount(t, 1, "logs", "list")

	// make sure latest log survived
	e.RunAndExpectSuccess(t, "logs", "show", thirdLogID)
}

func TestLogsMaintenance(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))
	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))
	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))
	e.RunAndVerifyOutputLineCount(t, 4, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--max-retained-log-count=2")
	e.RunAndVerifyOutputLineCount(t, 5, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "run")
	e.RunAndVerifyOutputLineCount(t, 2, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--max-retained-log-age=1ms")
	e.RunAndVerifyOutputLineCount(t, 3, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "run")
	e.RunAndVerifyOutputLineCount(t, 0, "logs", "list")
}
