// Package testenv contains Environment for use in testing.
package testenv

import (
	"bufio"
	"context"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/notification/sender"
	"github.com/kopia/kopia/notification/sender/testsender"
)

const (
	// TestRepoPassword is a password for repositories created in tests.
	TestRepoPassword = "qWQPJ2hiiLgWRRCr"
)

// CLIRunner encapsulates running kopia subcommands for testing purposes.
// It supports implementations that use subprocesses or in-process invocations.
type CLIRunner interface {
	Start(tb testing.TB, ctx context.Context, args []string, env map[string]string) (stdout, stderr io.Reader, wait func() error, interrupt func(os.Signal))
}

// CLITest encapsulates state for a CLI-based test.
type CLITest struct {
	// context in which all subcommands are running
	//nolint:containedctx
	RunContext context.Context

	startTime time.Time

	RepoDir   string
	ConfigDir string

	Runner CLIRunner

	fixedArgs   []string
	Environment map[string]string

	DefaultRepositoryCreateFlags []string

	logMu sync.RWMutex
	// +checklocks:logMu
	logOutputEnabled bool
	// +checklocks:logMu
	logOutputPrefix string
}

// RepoFormatNotImportant chooses arbitrary format version where it's not important to the test.
var RepoFormatNotImportant []string

// NewCLITest creates a new instance of *CLITest.
func NewCLITest(tb testing.TB, repoCreateFlags []string, runner CLIRunner) *CLITest {
	tb.Helper()
	configDir := testutil.TempDirectory(tb)

	// unset global environment variable that may interfere with the test
	os.Unsetenv("KOPIA_METRICS_PUSH_ADDR")

	fixedArgs := []string{
		// use per-test config file, to avoid clobbering current user's setup.
		"--config-file", filepath.Join(configDir, ".kopia.config"),
	}

	// disable the use of keyring
	switch runtime.GOOS {
	case "darwin":
		fixedArgs = append(fixedArgs, "--no-use-keychain")
	case "windows":
		fixedArgs = append(fixedArgs, "--no-use-credential-manager")
	case "linux":
		fixedArgs = append(fixedArgs, "--no-use-keyring")
	}

	var formatFlags []string

	formatFlags = append(formatFlags, repoCreateFlags...)

	if testutil.ShouldReduceTestComplexity() {
		formatFlags = append(formatFlags,
			"--encryption", "CHACHA20-POLY1305-HMAC-SHA256",
			"--block-hash", "BLAKE2S-256")
	}

	return &CLITest{
		RunContext:                   testsender.CaptureMessages(testlogging.Context(tb)),
		startTime:                    clock.Now(),
		RepoDir:                      testutil.TempDirectory(tb),
		ConfigDir:                    configDir,
		fixedArgs:                    fixedArgs,
		DefaultRepositoryCreateFlags: formatFlags,
		Environment: map[string]string{
			"KOPIA_PASSWORD": TestRepoPassword,
		},
		Runner: runner,
	}
}

// RunAndExpectSuccess runs the given command, expects it to succeed and returns its output lines.
func (e *CLITest) RunAndExpectSuccess(tb testing.TB, args ...string) []string {
	tb.Helper()

	stdout, _, err := e.Run(tb, false, args...)
	require.NoError(tb, err, "'kopia %v' failed", strings.Join(args, " "))

	return stdout
}

// TweakFile writes a xor-ed byte at a random point in a file.  Used to simulate file corruption.
func (e *CLITest) TweakFile(tb testing.TB, dirn, fglob string) {
	tb.Helper()

	const RwUserGroupOther = 0o666

	// find a file within the repository to corrupt
	mch, err := fs.Glob(os.DirFS(dirn), fglob)
	require.NoError(tb, err)
	require.NotEmpty(tb, mch)

	// grab a random file in the directory dirn
	fn := mch[rand.Intn(len(mch))]
	f, err := os.OpenFile(path.Join(dirn, fn), os.O_RDWR, os.FileMode(RwUserGroupOther))
	require.NoError(tb, err)

	// find the length of the file, then seek to a random location
	l, err := f.Seek(0, io.SeekEnd)
	require.NoError(tb, err)

	i := rand.Int63n(l)
	bs := [1]byte{}

	_, err = f.ReadAt(bs[:], i)
	require.NoError(tb, err)

	// write the byte
	_, err = f.WriteAt([]byte{^bs[0]}, i)
	require.NoError(tb, err)
}

func (e *CLITest) SetLogOutput(enable bool, prefix string) {
	e.logMu.Lock()
	defer e.logMu.Unlock()

	e.logOutputEnabled = enable
	e.logOutputPrefix = prefix
}

func (e *CLITest) NotificationsSent() []*sender.Message {
	return testsender.MessagesInContext(e.RunContext)
}

func (e *CLITest) getLogOutputPrefix() (string, bool) {
	e.logMu.RLock()
	defer e.logMu.RUnlock()

	return e.logOutputPrefix, os.Getenv("KOPIA_TEST_LOG_OUTPUT") != "" || e.logOutputEnabled
}

// RunAndProcessStderr runs the given command, and streams its stderr line-by-line to stderrCAllback until it returns false.
func (e *CLITest) RunAndProcessStderr(tb testing.TB, stderrCallback func(line string) bool, args ...string) (wait func() error, kill func()) {
	tb.Helper()

	wait, interrupt := e.RunAndProcessStderrInt(tb, stderrCallback, nil, args...)
	kill = func() {
		interrupt(os.Kill)
	}

	return wait, kill
}

// RunAndProcessStderrAsync runs the given command, and streams its stderr line-by-line stderrCAllback until it returns false.
func (e *CLITest) RunAndProcessStderrAsync(tb testing.TB, stderrCallback func(line string) bool, stderrAsyncCallback func(line string), args ...string) (wait func() error, kill func()) {
	tb.Helper()

	wait, interrupt := e.RunAndProcessStderrInt(tb, stderrCallback, stderrAsyncCallback, args...)
	kill = func() {
		interrupt(os.Kill)
	}

	return wait, kill
}

// RunAndProcessStderrInt runs the given command, and streams its stderr
// line-by-line to stderrCallback until it returns false. The remaining lines
// from stderr, if any, are asynchronously sent line-by-line to
// stderrAsyncCallback.
func (e *CLITest) RunAndProcessStderrInt(tb testing.TB, stderrCallback func(line string) bool, stderrAsyncCallback func(line string), args ...string) (wait func() error, interrupt func(os.Signal)) {
	tb.Helper()

	stdout, stderr, wait, interrupt := e.Runner.Start(tb, e.RunContext, e.cmdArgs(args), e.Environment)

	prefix, logOutput := e.getLogOutputPrefix()

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if logOutput {
				tb.Logf("[%vstdout] %v", prefix, scanner.Text())
			}
		}

		if logOutput {
			tb.Logf("[%vstdout] EOF", prefix)
		}
	}()

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		if !stderrCallback(scanner.Text()) {
			break
		}
	}

	// complete stderr scanning in the background without processing lines.
	go func() {
		for scanner.Scan() {
			if stderrAsyncCallback != nil {
				stderrAsyncCallback(scanner.Text())
			}

			if logOutput {
				tb.Logf("[%vstderr] %v", prefix, scanner.Text())
			}
		}

		if logOutput {
			tb.Logf("[%vstderr] EOF", prefix)
		}
	}()

	return wait, interrupt
}

// RunAndExpectSuccessWithErrOut runs the given command, expects it to succeed and returns its stdout and stderr lines.
func (e *CLITest) RunAndExpectSuccessWithErrOut(tb testing.TB, args ...string) (stdout, stderr []string) {
	tb.Helper()

	stdout, stderr, err := e.Run(tb, false, args...)
	require.NoError(tb, err, "'kopia %v' failed", strings.Join(args, " "))

	return stdout, stderr
}

// RunAndExpectFailure runs the given command, expects it to fail and returns its output lines.
func (e *CLITest) RunAndExpectFailure(tb testing.TB, args ...string) (stdout, stderr []string) {
	tb.Helper()

	var err error

	stdout, stderr, err = e.Run(tb, true, args...)
	require.Error(tb, err, "'kopia %v' succeeded, but expected failure", strings.Join(args, " "))

	return stdout, stderr
}

// RunAndVerifyOutputLineCount runs the given command and asserts it returns the given number of output lines, then returns them.
func (e *CLITest) RunAndVerifyOutputLineCount(tb testing.TB, wantLines int, args ...string) []string {
	tb.Helper()

	lines := e.RunAndExpectSuccess(tb, args...)
	require.Len(tb, lines, wantLines, "unexpected output lines for 'kopia %v', lines:\n %s", strings.Join(args, " "), strings.Join(lines, "\n "))

	return lines
}

func (e *CLITest) cmdArgs(args []string) []string {
	var suffix []string

	// detect repository creation and override DefaultRepositoryCreateFlags for best
	// performance on the current platform.
	if len(args) >= 2 && (args[0] == "repo" && args[1] == "create") {
		suffix = e.DefaultRepositoryCreateFlags
	}

	return append(append(append([]string(nil), e.fixedArgs...), args...), suffix...)
}

// Run executes kopia with given arguments and returns the output lines.
func (e *CLITest) Run(tb testing.TB, expectedError bool, args ...string) (stdout, stderr []string, err error) {
	tb.Helper()

	args = e.cmdArgs(args)
	outputPrefix, logOutput := e.getLogOutputPrefix()
	tb.Logf("%vrunning 'kopia %v' with %v", outputPrefix, strings.Join(args, " "), e.Environment)

	timer := timetrack.StartTimer()

	stdoutReader, stderrReader, wait, _ := e.Runner.Start(tb, e.RunContext, args, e.Environment)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			if logOutput {
				tb.Logf("[%vstdout] %v", outputPrefix, scanner.Text())
			}

			stdout = append(stdout, scanner.Text())
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stderrReader)
		for scanner.Scan() {
			if logOutput {
				tb.Logf("[%vstderr] %v", outputPrefix, scanner.Text())
			}

			stderr = append(stderr, scanner.Text())
		}
	}()

	wg.Wait()

	gotErr := wait()

	if expectedError {
		require.Error(tb, gotErr, "unexpected success when running 'kopia %v' (stdout:\n%v\nstderr:\n%v", strings.Join(args, " "), strings.Join(stdout, "\n"), strings.Join(stderr, "\n"))
	} else {
		require.NoError(tb, gotErr, "unexpected error when running 'kopia %v' (stdout:\n%v\nstderr:\n%v", strings.Join(args, " "), strings.Join(stdout, "\n"), strings.Join(stderr, "\n"))
	}

	//nolint:forbidigo
	tb.Logf("%vfinished in %v: 'kopia %v'", outputPrefix, timer.Elapsed().Milliseconds(), strings.Join(args, " "))

	return stdout, stderr, gotErr
}
