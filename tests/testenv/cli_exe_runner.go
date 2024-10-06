package testenv

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/internal/testutil"
)

// CLIExeRunner is a CLIExeRunner that invokes the commands via external executable.
type CLIExeRunner struct {
	Exe               string
	PassthroughStderr bool       // this is for debugging only
	NextCommandStdin  io.Reader  // this is used for stdin source tests
	ExtraFiles        []*os.File // this is used for socket-activation tests
	LogsDir           string
}

// Start implements CLIRunner.
func (e *CLIExeRunner) Start(t *testing.T, ctx context.Context, args []string, env map[string]string) (stdout, stderr io.Reader, wait func() error, interrupt func(os.Signal)) {
	t.Helper()

	c := exec.Command(e.Exe, append([]string{
		"--log-dir", e.LogsDir,
	}, args...)...)

	c.Env = append(c.Env, os.Environ()...)

	for k, v := range env {
		c.Env = append(c.Env, k+"="+v)
	}

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
	c.ExtraFiles = e.ExtraFiles

	if err := c.Start(); err != nil {
		t.Fatalf("unable to start: %v", err)
	}

	return stdoutPipe, stderrPipe, c.Wait, func(sig os.Signal) {
		if sig == os.Kill {
			c.Process.Kill()

			return
		}

		c.Process.Signal(sig)
	}
}

// NewExeRunner returns a CLIRunner that will execute kopia commands by launching subprocesses
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

	return NewExeRunnerWithBinary(t, exe)
}

// NewExeRunnerWithBinary returns a CLIRunner that will execute kopia commands by launching subprocesses
// for each.
func NewExeRunnerWithBinary(t *testing.T, exe string) *CLIExeRunner {
	t.Helper()

	// unset environment variables that disrupt tests when passed to subprocesses.
	os.Unsetenv("KOPIA_PASSWORD")

	logsDir := testutil.TempLogDirectory(t)

	return &CLIExeRunner{
		Exe:     filepath.FromSlash(exe),
		LogsDir: logsDir,
	}
}

var _ CLIRunner = (*CLIExeRunner)(nil)
