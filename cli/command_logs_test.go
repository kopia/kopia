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
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

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

	lines2 := e.RunAndVerifyOutputLineCount(t, 1, "logs", "list", "-n1")
	require.Equal(t, thirdLogID, strings.Split(lines2[0], " ")[0])

	e.RunAndVerifyOutputLineCount(t, 0, "logs", "list", "--younger-than=1ms")
	e.RunAndVerifyOutputLineCount(t, 3, "logs", "list", "--younger-than=1h")
	e.RunAndVerifyOutputLineCount(t, 3, "logs", "list", "--older-than=1ms")
	e.RunAndVerifyOutputLineCount(t, 0, "logs", "list", "--older-than=1h")

	require.NotEqual(t, firstLogLines, secondLogLines)
	require.NotEqual(t, secondLogLines, thirdLogLines)

	// by default cleanup retains a lot of logs.
	e.RunAndExpectSuccess(t, "logs", "cleanup")
	e.RunAndVerifyOutputLineCount(t, 3, "logs", "list")
	e.RunAndExpectSuccess(t, "logs", "cleanup", "--max-count=2", "--dry-run")
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
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	time.Sleep(time.Second)
	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))
	time.Sleep(time.Second)
	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))
	time.Sleep(time.Second)
	e.RunAndExpectSuccess(t, "snapshot", "create", testutil.TempDirectory(t))
	e.RunAndVerifyOutputLineCount(t, 4, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--max-retained-log-count=2")
	e.RunAndVerifyOutputLineCount(t, 5, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "run", "--full")
	e.RunAndVerifyOutputLineCount(t, 3, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--max-retained-log-age=1ms")
	e.RunAndVerifyOutputLineCount(t, 4, "logs", "list")

	e.RunAndExpectSuccess(t, "maintenance", "run", "--full")
	e.RunAndVerifyOutputLineCount(t, 1, "logs", "list")
}

func TestLogsMaintenanceSet(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "maintenance", "set",
		"--max-retained-log-age=22h",
		"--max-retained-log-size-mb=33",
		"--max-retained-log-count=44",
	)

	infoLines := e.RunAndExpectSuccess(t, "maintenance", "info")
	require.Contains(t, infoLines, "  max age of logs: 22h0m0s")
	require.Contains(t, infoLines, "  max total size:  34.6 MB")
	require.Contains(t, infoLines, "  max count:       44")
}
