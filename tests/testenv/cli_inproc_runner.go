package testenv

import (
	"io"
	"os"
	"testing"

	"github.com/alecthomas/kingpin"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testlogging"
)

// CLIInProcRunner is a CLIRunner that invokes provided commands in the current process.
type CLIInProcRunner struct {
	RepoPassword string

	NextCommandStdin io.Reader // this is used for stdin source tests

	CustomizeApp func(a *cli.App, kp *kingpin.Application)
}

// Start implements CLIRunner.
func (e *CLIInProcRunner) Start(t *testing.T, args []string) (stdout, stderr io.Reader, wait func() error, kill func()) {
	t.Helper()

	ctx := testlogging.Context(t)

	a := cli.NewApp()
	a.AdvancedCommands = "enabled"

	kpapp := kingpin.New("test", "test")

	if e.CustomizeApp != nil {
		e.CustomizeApp(a, kpapp)
	}

	stdin := e.NextCommandStdin
	e.NextCommandStdin = nil

	return a.RunSubcommand(ctx, kpapp, stdin, append([]string{
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
