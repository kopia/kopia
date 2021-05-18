package testenv

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kopia/kopia/internal/clock"
)

// CLIExeRunner is a CLIExeRunner that invokes the commands via external executable.
type CLIExeRunner struct {
	Exe               string
	Environment       []string
	PassthroughStderr bool      // this is for debugging only
	NextCommandStdin  io.Reader // this is used for stdin source tests
	LogsDir           string
}

// Start implements CLIRunner.
func (e *CLIExeRunner) Start(t *testing.T, args []string) (stdout, stderr io.Reader, wait func() error, kill func()) {
	t.Helper()

	c := exec.Command(e.Exe, append([]string{
		"--log-dir", e.LogsDir,
	}, args...)...)

	c.Env = append(os.Environ(), e.Environment...)

	stdoutPipe, err := c.StdoutPipe()
	if err != nil {
		t.Fatalf("can't set up stdout pipe reader: %v", err)
	}

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		t.Fatalf("can't set up stderr pipe reader: %v", err)
	}

	c.Stdin = e.NextCommandStdin
	e.NextCommandStdin = nil

	if err := c.Start(); err != nil {
		t.Fatalf("unable to start: %v", err)
	}

	return stdoutPipe, stderrPipe, c.Wait, func() {
		c.Process.Kill()
	}
}

// RemoveDefaultPassword prevents KOPIA_PASSWORD from being passed to kopia.
func (e *CLIExeRunner) RemoveDefaultPassword() {
	var newEnv []string

	for _, s := range e.Environment {
		if !strings.HasPrefix(s, "KOPIA_PASSWORD=") {
			newEnv = append(newEnv, s)
		}
	}

	e.Environment = newEnv
}

// NewExeRunner resutns a CLIRunner that will execute kopia commands by launching subprocesses
// for each. The kopia executable must be passed via KOPIA_EXE environment variable. The test
// will be skipped if it's not provided (unless running inside an IDE in which case system-wide
// `kopia` will be used by default).
func NewExeRunner(t *testing.T) *CLIExeRunner {
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
			t.Logf("FAILURE ABOVE ^^^^")
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

	return &CLIExeRunner{
		Exe: filepath.FromSlash(exe),
		Environment: []string{
			"KOPIA_PASSWORD=" + TestRepoPassword,
			"KOPIA_ADVANCED_COMMANDS=enabled",
		},
		LogsDir: logsDir,
	}
}

var _ CLIRunner = (*CLIExeRunner)(nil)
