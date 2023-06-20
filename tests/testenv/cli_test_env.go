// Package testenv contains Environment for use in testing.
package testenv

import (
	"bufio"
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
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/timetrack"
)

const (
	// TestRepoPassword is a password for repositories created in tests.
	TestRepoPassword = "qWQPJ2hiiLgWRRCr"
)

// CLIRunner encapsulates running kopia subcommands for testing purposes.
// It supports implementations that use subprocesses or in-process invocations.
type CLIRunner interface {
	Start(t *testing.T, args []string, env map[string]string) (stdout, stderr io.Reader, wait func() error, kill func())
}

// CLITest encapsulates state for a CLI-based test.
type CLITest struct {
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
func NewCLITest(t *testing.T, repoCreateFlags []string, runner CLIRunner) *CLITest {
	t.Helper()
	configDir := testutil.TempDirectory(t)

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
		startTime:                    clock.Now(),
		RepoDir:                      testutil.TempDirectory(t),
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
func (e *CLITest) RunAndExpectSuccess(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, false, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout
}

// TweakFile writes a xor-ed byte at a random point in a file.  Used to simulate file corruption.
func (e *CLITest) TweakFile(t *testing.T, dirn, fglob string) {
	t.Helper()

	const RwUserGroupOther = 0o666

	// find a file within the repository to corrupt
	mch, err := fs.Glob(os.DirFS(dirn), fglob)
	require.NoError(t, err)
	require.Greater(t, len(mch), 0)

	// grab a random file in the directory dirn
	fn := mch[rand.Intn(len(mch))]
	f, err := os.OpenFile(path.Join(dirn, fn), os.O_RDWR, os.FileMode(RwUserGroupOther))
	require.NoError(t, err)

	// find the length of the file, then seek to a random location
	l, err := f.Seek(0, io.SeekEnd)
	require.NoError(t, err)

	i := rand.Int63n(l)
	bs := [1]byte{}

	_, err = f.ReadAt(bs[:], i)
	require.NoError(t, err)

	// write the byte
	_, err = f.WriteAt([]byte{^bs[0]}, i)
	require.NoError(t, err)
}

func (e *CLITest) SetLogOutput(enable bool, prefix string) {
	e.logMu.Lock()
	defer e.logMu.Unlock()

	e.logOutputEnabled = enable
	e.logOutputPrefix = prefix
}

func (e *CLITest) getLogOutputPrefix() (string, bool) {
	e.logMu.RLock()
	defer e.logMu.RUnlock()

	return e.logOutputPrefix, os.Getenv("KOPIA_TEST_LOG_OUTPUT") != "" || e.logOutputEnabled
}

// RunAndProcessStderr runs the given command, and streams its output line-by-line to a given function until it returns false.
func (e *CLITest) RunAndProcessStderr(t *testing.T, callback func(line string) bool, args ...string) (wait func() error, kill func()) {
	t.Helper()

	stdout, stderr, wait, kill := e.Runner.Start(t, e.cmdArgs(args), e.Environment)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if prefix, ok := e.getLogOutputPrefix(); ok {
				t.Logf("[%vstdout] %v", prefix, scanner.Text())
			}
		}

		if prefix, ok := e.getLogOutputPrefix(); ok {
			t.Logf("[%vstdout] EOF", prefix)
		}
	}()

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		if !callback(scanner.Text()) {
			break
		}
	}

	// complete the scan in background without processing lines.
	go func() {
		for scanner.Scan() {
			if prefix, ok := e.getLogOutputPrefix(); ok {
				t.Logf("[%vstderr] %v", prefix, scanner.Text())
			}
		}

		if prefix, ok := e.getLogOutputPrefix(); ok {
			t.Logf("[%vstderr] EOF", prefix)
		}
	}()

	return wait, kill
}

// RunAndExpectSuccessWithErrOut runs the given command, expects it to succeed and returns its stdout and stderr lines.
func (e *CLITest) RunAndExpectSuccessWithErrOut(t *testing.T, args ...string) (stdout, stderr []string) {
	t.Helper()

	stdout, stderr, err := e.Run(t, false, args...)
	if err != nil {
		t.Fatalf("'kopia %v' failed with %v", strings.Join(args, " "), err)
	}

	return stdout, stderr
}

// RunAndExpectFailure runs the given command, expects it to fail and returns its output lines.
func (e *CLITest) RunAndExpectFailure(t *testing.T, args ...string) (stdout, stderr []string) {
	t.Helper()

	var err error

	stdout, stderr, err = e.Run(t, true, args...)
	if err == nil {
		t.Fatalf("'kopia %v' succeeded, but expected failure", strings.Join(args, " "))
	}

	return stdout, stderr
}

// RunAndVerifyOutputLineCount runs the given command and asserts it returns the given number of output lines, then returns them.
func (e *CLITest) RunAndVerifyOutputLineCount(t *testing.T, wantLines int, args ...string) []string {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, args...)
	if len(lines) != wantLines {
		t.Fatalf("unexpected list of results of 'kopia %v': %v lines (%v) wanted %v", strings.Join(args, " "), len(lines), lines, wantLines)
	}

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
func (e *CLITest) Run(t *testing.T, expectedError bool, args ...string) (stdout, stderr []string, err error) {
	t.Helper()

	args = e.cmdArgs(args)
	outputPrefix, logOutput := e.getLogOutputPrefix()
	t.Logf("%vrunning 'kopia %v' with %v", outputPrefix, strings.Join(args, " "), e.Environment)

	timer := timetrack.StartTimer()

	stdoutReader, stderrReader, wait, _ := e.Runner.Start(t, args, e.Environment)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			if logOutput {
				t.Logf("[%vstdout] %v", outputPrefix, scanner.Text())
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
				t.Logf("[%vstderr] %v", outputPrefix, scanner.Text())
			}

			stderr = append(stderr, scanner.Text())
		}
	}()

	wg.Wait()

	gotErr := wait()

	if expectedError {
		require.Error(t, gotErr, "unexpected success when running 'kopia %v' (stdout:\n%v\nstderr:\n%v", strings.Join(args, " "), strings.Join(stdout, "\n"), strings.Join(stderr, "\n"))
	} else {
		require.NoError(t, gotErr, "unexpected error when running 'kopia %v' (stdout:\n%v\nstderr:\n%v", strings.Join(args, " "), strings.Join(stdout, "\n"), strings.Join(stderr, "\n"))
	}

	//nolint:forbidigo
	t.Logf("%vfinished in %v: 'kopia %v'", outputPrefix, timer.Elapsed().Milliseconds(), strings.Join(args, " "))

	return stdout, stderr, gotErr
}
