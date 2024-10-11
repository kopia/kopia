package testenv

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/cli"
)

var envPrefixCounter = new(int32)

// CLIInProcRunner is a CLIRunner that invokes provided commands in the current process.
type CLIInProcRunner struct {
	mu sync.Mutex

	// +checklocks:mu
	nextCommandStdin io.Reader // this is used for stdin source tests

	CustomizeApp func(a *cli.App, kp *kingpin.Application)
}

// Start implements CLIRunner.
func (e *CLIInProcRunner) Start(t *testing.T, ctx context.Context, args []string, env map[string]string) (stdout, stderr io.Reader, wait func() error, interrupt func(os.Signal)) {
	t.Helper()

	a := cli.NewApp()
	a.AdvancedCommands = "enabled"

	envPrefix := fmt.Sprintf("T%v_", atomic.AddInt32(envPrefixCounter, 1))
	a.SetEnvNamePrefixForTesting(envPrefix)

	kpapp := kingpin.New("test", "test")

	if e.CustomizeApp != nil {
		e.CustomizeApp(a, kpapp)
	}

	e.mu.Lock()
	stdin := e.nextCommandStdin
	e.nextCommandStdin = nil
	e.mu.Unlock()

	for k, v := range env {
		os.Setenv(envPrefix+k, v)
	}

	return a.RunSubcommand(ctx, kpapp, stdin, args)
}

// SetNextStdin sets the stdin to be used on next command execution.
func (e *CLIInProcRunner) SetNextStdin(stdin io.Reader) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextCommandStdin = stdin
}

// NewInProcRunner returns a runner that executes CLI subcommands in the current process using cli.RunSubcommand().
func NewInProcRunner(t *testing.T) *CLIInProcRunner {
	t.Helper()

	if os.Getenv("KOPIA_EXE") != "" && os.Getenv("KOPIA_RUN_ALL_INTEGRATION_TESTS") == "" {
		t.Skip("not running test since it's also included in the unit tests")
	}

	return &CLIInProcRunner{
		CustomizeApp: func(a *cli.App, kp *kingpin.Application) {
			a.AddStorageProvider(cli.StorageProvider{
				Name:        "in-memory",
				Description: "in-memory storage backend",
				NewFlags:    func() cli.StorageFlags { return &storageInMemoryFlags{} },
			})
		},
	}
}

var _ CLIRunner = (*CLIInProcRunner)(nil)
