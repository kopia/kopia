package logfile_test

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/logfile"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

var (
	cliLogFormat              = regexp.MustCompile(`^\d{4}-\d\d\-\d\dT\d\d:\d\d:\d\d\.\d{6}Z (DEBUG|INFO) [a-z/]+ .*$`)
	contentLogFormat          = regexp.MustCompile(`^\d{4}-\d\d\-\d\dT\d\d:\d\d:\d\d\.\d{6}Z .*$`)
	cliLogFormatLocalTimezone = regexp.MustCompile(`^\d{4}-\d\d\-\d\dT\d\d:\d\d:\d\d\.\d{6}[^Z][^ ]+ (DEBUG|INFO) [a-z/]+ .*$`)
)

func TestLoggingFlags(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	runner.CustomizeApp = logfile.Attach

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	dir1 := testutil.TempDirectory(t)

	tmpLogDir := testutil.TempDirectory(t)

	// run command that produces a mix of debug and info logs.
	_, stderr, err := env.Run(t, false, "snap", "create", dir1,
		"--console-timestamps", "--no-progress", "--log-level=debug", "--force-color",
		"--no-auto-maintenance", "--log-dir", tmpLogDir)
	require.NoError(t, err)

	for _, l := range stderr {
		require.NotContains(t, l, "INFO") // INFO is omitted

		if strings.Contains(l, "DEBUG") {
			require.Contains(t, l, "\x1b[35mDEBUG\x1b")
		}

		// make sure each line is prefixed with a timestamp.
		_, perr := time.Parse("15:04:05.000 ", strings.Split(l, " ")[0])
		require.NoError(t, perr)
	}

	verifyFileLogFormat(t, filepath.Join(tmpLogDir, "cli-logs", "latest.log"), cliLogFormat)
	verifyFileLogFormat(t, filepath.Join(tmpLogDir, "content-logs", "latest.log"), contentLogFormat)

	_, stderr, err = env.Run(t, false, "snap", "create", dir1,
		"--file-log-local-tz", "--no-progress", "--log-level=debug", "--disable-color",
		"--no-auto-maintenance", "--log-dir", tmpLogDir)
	require.NoError(t, err)

	if isUTC() {
		verifyFileLogFormat(t, filepath.Join(tmpLogDir, "cli-logs", "latest.log"), cliLogFormat)
	} else {
		verifyFileLogFormat(t, filepath.Join(tmpLogDir, "cli-logs", "latest.log"), cliLogFormatLocalTimezone)
	}

	verifyFileLogFormat(t, filepath.Join(tmpLogDir, "content-logs", "latest.log"), contentLogFormat)

	for _, l := range stderr {
		require.NotContains(t, l, "INFO") // INFO is omitted

		if strings.Contains(l, "DEBUG") {
			require.NotContains(t, l, "\x1b[35mDEBUG")
		}

		// make sure each line is NOT prefixed with a timestamp.
		_, perr := time.Parse("15:04:05.000 ", strings.Split(l, " ")[0])
		require.Error(t, perr)
	}

	// run command with --log-level=warning so no log error is produced on the console
	_, stderr, err = env.Run(t, false, "snap", "create", dir1,
		"--no-progress", "--log-level=warning",
		"--no-auto-maintenance", "--log-dir", tmpLogDir)
	require.NoError(t, err)
	require.Empty(t, stderr)

	// run command with --log-level=error so no log error is produced on the console
	_, stderr, err = env.Run(t, false, "snap", "create", dir1,
		"--no-progress", "--log-level=error",
		"--no-auto-maintenance", "--log-dir", tmpLogDir)
	require.NoError(t, err)
	require.Empty(t, stderr)
}

func TestLogFileRotation(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	runner.CustomizeApp = logfile.Attach

	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	dir1 := testutil.TempDirectory(t)

	tmpLogDir := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "snap", "create", dir1,
		"--file-log-local-tz", "--log-level=error", "--file-log-level=debug",
		"--max-log-file-segment-size=1000", "--log-dir", tmpLogDir, "--log-dir-max-files=3", "--content-log-dir-max-files=4")

	// expected number of files per directory
	subdirs := map[string]int{
		"cli-logs":     3,
		"content-logs": 4,
	}

	for subdir, wantEntryCount := range subdirs {
		logSubdir := filepath.Join(tmpLogDir, subdir)
		wantEntryCount := wantEntryCount

		t.Run(subdir, func(t *testing.T) {
			entries, err := os.ReadDir(logSubdir)
			require.NoError(t, err)

			var gotEntryCount int

			for _, ent := range entries {
				info, err := ent.Info()
				require.NoError(t, err)

				t.Logf("%v %v", info.Name(), info.Size())
				if info.Mode().IsRegular() {
					gotEntryCount++
				}

				require.LessOrEqual(t, info.Size(), int64(3000), info.Name())
			}

			require.Equal(t, wantEntryCount, gotEntryCount)
		})
	}
}

func verifyFileLogFormat(t *testing.T, fname string, re *regexp.Regexp) {
	t.Helper()

	f, err := os.Open(fname)
	require.NoError(t, err)

	defer f.Close()

	s := bufio.NewScanner(f)

	for s.Scan() {
		require.True(t, re.MatchString(s.Text()), "log line does not match the format: %v", s.Text())
	}
}

func isUTC() bool {
	_, offset := clock.Now().Zone()

	return offset == 0
}
