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

// TweakFile writes a 0x00 byte at a random point in a file.  Used to simulate file corruption
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
	for {
		// find a location that isn't already
		// the value we want to write
		_, err := f.ReadAt(bs[:], i)
		require.NoError(t, err)
		if bs[0] != 0x00 {
			break
		}
		i = rand.Int63n(l)
	}
	// write the byte
	_, err = f.WriteAt([]byte{0x00}, i)
	require.NoError(t, err)
}

// RunAndProcessStderr runs the given command, and streams its output line-by-line to a given function until it returns false.
func (e *CLITest) RunAndProcessStderr(t *testing.T, callback func(line string) bool, args ...string) (wait func() error, kill func()) {
	t.Helper()

	stdout, stderr, wait, kill := e.Runner.Start(t, e.cmdArgs(args), e.Environment)
	go io.Copy(io.Discard, stdout)

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		if !callback(scanner.Text()) {
			break
		}
	}

	// complete the scan in background without processing lines.
	go func() {
		for scanner.Scan() {
			// ignore
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
	t.Logf("running 'kopia %v' with %v", strings.Join(args, " "), e.Environment)

	timer := timetrack.StartTimer()

	stdoutReader, stderrReader, wait, _ := e.Runner.Start(t, args, e.Environment)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			if os.Getenv("KOPIA_TEST_LOG_OUTPUT") != "" {
				t.Logf("[stdout] %v", scanner.Text())
			}

			stdout = append(stdout, scanner.Text())
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stderrReader)
		for scanner.Scan() {
			if os.Getenv("KOPIA_TEST_LOG_OUTPUT") != "" {
				t.Logf("[stderr] %v", scanner.Text())
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
	t.Logf("finished in %v: 'kopia %v'", timer.Elapsed().Milliseconds(), strings.Join(args, " "))

	return stdout, stderr, gotErr
}
