package testenv

import (
	"io"
	"os"
	"testing"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testlogging"
)

// CLIInProcRunner is a CLIRunner that invokes provided commands in the current process.
type CLIInProcRunner struct{}

// Start implements CLIRunner.
func (e *CLIInProcRunner) Start(t *testing.T, args []string) (stdout, stderr io.Reader, wait func() error, kill func()) {
	t.Helper()

	ctx := testlogging.Context(t)

	return cli.RunSubcommand(ctx, append([]string{
		"--password", TestRepoPassword,
		"--advanced-commands=enabled",
	}, args...))
}

// NewInProcRunner returns a runner that executes CLI subcommands in the current process using cli.RunSubcommand().
func NewInProcRunner(t *testing.T) *CLIInProcRunner {
	t.Helper()

	if os.Getenv("KOPIA_EXE") != "" {
		t.Skip("not running test since it's also included in the unit tests")
	}

	return &CLIInProcRunner{}
}

var _ CLIRunner = (*CLIInProcRunner)(nil)
