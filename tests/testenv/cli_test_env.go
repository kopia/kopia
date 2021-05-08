// Package testenv contains Environment for use in testing.
package testenv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testutil"
)

const (
	// TestRepoPassword is a password for repositories created in tests.
	TestRepoPassword = "qWQPJ2hiiLgWRRCr"

	maxOutputLinesToLog = 4000
)

// CLITest encapsulates state for a CLI-based test.
type CLITest struct {
	startTime time.Time

	RepoDir   string
	ConfigDir string
	Exe       string

	fixedArgs   []string
	Environment []string

	DefaultRepositoryCreateFlags []string

	PassthroughStderr bool // this is for debugging only

	NextCommandStdin io.Reader // this is used for stdin source tests

	LogsDir string
}

// NewCLITest creates a new instance of *CLITest.
func NewCLITest(t *testing.T) *CLITest {
	t.Helper()

	exe := os.Getenv("KOPIA_EXE")
	if exe == "" {
		if os.Getenv("VSCODE_PID") != "" {
			// we're launched from VSCode, use system-installed kopia executable.
			exe = "kopia"
		} else {
			t.Skip()
		}
	}

	// unset environment variables that disrupt tests when passed to subprocesses.
	os.Unsetenv("KOPIA_PASSWORD")

	configDir := testutil.TempDirectory(t)

	cleanName := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(
		t.Name(),
		"/", "_"), "\\", "_"), ":", "_")

	logsBaseDir := os.Getenv("KOPIA_LOGS_DIR")
	if logsBaseDir == "" {
		logsBaseDir = filepath.Join(os.TempDir(), "kopia-logs")
	}

	logsDir := filepath.Join(logsBaseDir, cleanName+"."+clock.Now().Local().Format("20060102150405"))

	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("FAULURE ABOVE ^^^^")
		}

		if os.Getenv("KOPIA_KEEP_LOGS") != "" {
			t.Logf("logs preserved in %v", logsDir)
			return
		}

		if t.Failed() && os.Getenv("KOPIA_DISABLE_LOG_DUMP_ON_FAILURE") == "" {
			dumpLogs(t, logsDir)
		}

		os.RemoveAll(logsDir)
	})

	fixedArgs := []string{
		// use per-test config file, to avoid clobbering current user's setup.
		"--config-file", filepath.Join(configDir, ".kopia.config"),
		"--log-dir", logsDir,
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

	if testutil.ShouldReduceTestComplexity() {
		formatFlags = []string{
			"--encryption", "CHACHA20-POLY1305-HMAC-SHA256",
			"--block-hash", "BLAKE2S-256",
		}
	}

	return &CLITest{
		startTime:                    clock.Now(),
		RepoDir:                      testutil.TempDirectory(t),
		ConfigDir:                    configDir,
		Exe:                          filepath.FromSlash(exe),
		fixedArgs:                    fixedArgs,
		DefaultRepositoryCreateFlags: formatFlags,
		LogsDir:                      logsDir,
		Environment: []string{
			"KOPIA_PASSWORD=" + TestRepoPassword,
			"KOPIA_ADVANCED_COMMANDS=enabled",
		},
	}
}

func dumpLogs(t *testing.T, dirname string) {
	t.Helper()

	entries, err := ioutil.ReadDir(dirname)
	if err != nil {
		t.Errorf("unable to read %v: %v", dirname, err)

		return
	}

	for _, e := range entries {
		if e.IsDir() {
			dumpLogs(t, filepath.Join(dirname, e.Name()))
			continue
		}

		dumpLogFile(t, filepath.Join(dirname, e.Name()))
	}
}

func dumpLogFile(t *testing.T, fname string) {
	t.Helper()

	data, err := ioutil.ReadFile(fname)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("LOG FILE: %v %v", fname, trimOutput(string(data)))
}

// RemoveDefaultPassword prevents KOPIA_PASSWORD from being passed to kopia.
func (e *CLITest) RemoveDefaultPassword() {
	var newEnv []string

	for _, s := range e.Environment {
		if !strings.HasPrefix(s, "KOPIA_PASSWORD=") {
			newEnv = append(newEnv, s)
		}
	}

	e.Environment = newEnv
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

// RunAndProcessStderr runs the given command, and streams its output line-by-line to a given function until it returns false.
func (e *CLITest) RunAndProcessStderr(t *testing.T, callback func(line string) bool, args ...string) *exec.Cmd {
	t.Helper()

	c := exec.Command(e.Exe, e.cmdArgs(args)...)
	c.Env = append(os.Environ(), e.Environment...)
	t.Logf("running '%v %v'", c.Path, c.Args)

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		t.Fatalf("can't set up stderr pipe reader")
	}

	if err := c.Start(); err != nil {
		t.Fatalf("unable to start")
	}

	scanner := bufio.NewScanner(stderrPipe)
	for scanner.Scan() {
		if !callback(scanner.Text()) {
			break
		}
	}

	// complete the scan in background without processing lines.
	go func() {
		for scanner.Scan() {
			t.Logf("[stderr] %v", scanner.Text())
		}
	}()

	return c
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
func (e *CLITest) RunAndExpectFailure(t *testing.T, args ...string) []string {
	t.Helper()

	stdout, _, err := e.Run(t, true, args...)
	if err == nil {
		t.Fatalf("'kopia %v' succeeded, but expected failure", strings.Join(args, " "))
	}

	return stdout
}

// RunAndVerifyOutputLineCount runs the given command and asserts it returns the given number of output lines, then returns them.
func (e *CLITest) RunAndVerifyOutputLineCount(t *testing.T, wantLines int, args ...string) []string {
	t.Helper()

	lines := e.RunAndExpectSuccess(t, args...)
	if len(lines) != wantLines {
		t.Errorf("unexpected list of results of 'kopia %v': %v (%v lines), wanted %v", strings.Join(args, " "), lines, len(lines), wantLines)
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

	c := exec.Command(e.Exe, e.cmdArgs(args)...)
	c.Env = append(os.Environ(), e.Environment...)

	t.Logf("running '%v %v'", c.Path, c.Args)

	errOut := &bytes.Buffer{}
	c.Stderr = errOut

	if e.PassthroughStderr {
		c.Stderr = os.Stderr
	}

	c.Stdin = e.NextCommandStdin
	e.NextCommandStdin = nil

	o, err := c.Output()

	if err != nil && !expectedError {
		t.Logf("finished 'kopia %v' with err=%v (expected=%v) and output:\n%v\nstderr:\n%v\n", strings.Join(args, " "), err, expectedError, trimOutput(string(o)), trimOutput(errOut.String()))
	}

	return splitLines(string(o)), splitLines(errOut.String()), err
}

func trimOutput(s string) string {
	lines := splitLines(s)
	if len(lines) <= maxOutputLinesToLog {
		return s
	}

	lines2 := append([]string(nil), lines[0:(maxOutputLinesToLog/2)]...)
	lines2 = append(lines2, fmt.Sprintf("/* %v lines removed */", len(lines)-maxOutputLinesToLog))
	lines2 = append(lines2, lines[len(lines)-(maxOutputLinesToLog/2):]...)

	return strings.Join(lines2, "\n")
}

func splitLines(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	var result []string
	for _, l := range strings.Split(s, "\n") {
		result = append(result, strings.TrimRight(l, "\r"))
	}

	return result
}
