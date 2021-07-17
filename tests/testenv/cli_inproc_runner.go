package testenv

import (
	"io"
	"os"
	"testing"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/buf"
	"github.com/kopia/kopia/internal/testlogging"
)

// CLIInProcRunner is a CLIRunner that invokes provided commands in the current process.
type CLIInProcRunner struct {
	RepoPassword string
}

// Start implements CLIRunner.
func (e *CLIInProcRunner) Start(t *testing.T, args []string) (stdout, stderr io.Reader, wait func() error, kill func()) {
	t.Helper()

	ctx := testlogging.Context(t)

	a := cli.NewApp()
	a.AdvancedCommands = "enabled"

	return a.RunSubcommand(ctx, append([]string{
		"--password", e.RepoPassword,
	}, args...))
}

// NewInProcRunner returns a runner that executes CLI subcommands in the current process using cli.RunSubcommand().
func NewInProcRunner(t *testing.T) *CLIInProcRunner {
	t.Helper()

	if os.Getenv("KOPIA_EXE") != "" && os.Getenv("KOPIA_RUN_ALL_INTEGRATION_TESTS") == "" {
		t.Skip("not running test since it's also included in the unit tests")
	}

	return &CLIInProcRunner{
		RepoPassword: TestRepoPassword,
	}
}

var _ CLIRunner = (*CLIInProcRunner)(nil)

func init() {
	// disable buffer management in end-to-end tests as running too many of them in parallel causes too
	// much memory usage on low-end platforms.
	buf.DisableBufferManagement = true
}
