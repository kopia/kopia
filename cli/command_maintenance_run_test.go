package cli_test

import (
	"path/filepath"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/logfile"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/tests/testenv"
)

func TestMaintenanceRunReportsInvalidGlobalFlag(t *testing.T) {
	app := cli.NewApp()
	parser := kingpin.New("kopia", "")
	logfile.Attach(app, parser)
	app.Attach(parser)

	_, err := parser.Parse([]string{"--log-level=warn", "maintenance", "run", "--safety=none"})

	require.ErrorContains(t, err, "enum value must be one of debug,info,warning,error, got 'warn'")
}

func TestMaintenanceRunCompactionOptions(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	out := e.RunAndExpectSuccess(t, "cache", "info")
	require.Contains(t, mustGetLineContaining(t, out, "Index compaction working-memory budget:"), units.BytesString(512<<20))
	require.EqualValues(t, 512<<20, indexblob.DefaultEpochCompactionMemoryLimit)

	e.RunAndExpectSuccess(t, "cache", "set", "--index-compaction-memory=256MB")
	configFile := filepath.Join(e.ConfigDir, ".kopia.config")
	assertSavedBudget := func() {
		t.Helper()

		opts, err := repo.GetCachingOptions(e.RunContext, configFile)
		require.NoError(t, err)
		require.EqualValues(t, 256<<20, opts.IndexCompactionMemoryLimitBytes)
	}
	assertSavedBudget()
	// Editing other cache settings must preserve the compaction budget.
	e.RunAndExpectSuccess(t, "cache", "set", "--max-list-cache-duration=55s")
	assertSavedBudget()

	out = e.RunAndExpectSuccess(t, "cache", "info")
	require.Contains(t, mustGetLineContaining(t, out, "Index compaction working-memory budget:"), units.BytesString(256<<20))

	e.RunAndExpectSuccess(t, "maintenance", "run")
	e.RunAndExpectSuccess(t, "maintenance", "run", "--index-compaction-memory=1MB")
	e.RunAndExpectSuccess(t, "maintenance", "run", "--full", "--index-compaction-memory=1MB")
	assertSavedBudget()
	e.RunAndExpectFailure(t, "maintenance", "run", "--index-compaction-memory=1048575B")
	e.RunAndExpectFailure(t, "maintenance", "run", "--index-compaction-memory=64KB")
	e.RunAndExpectFailure(t, "maintenance", "run", "--index-compaction-memory=1KB")
	e.RunAndExpectFailure(t, "maintenance", "run", "--index-compaction-memory=0")

	for _, value := range []string{"0", "-1MB", "1048575B"} {
		e.RunAndExpectFailure(t, "cache", "set", "--index-compaction-memory="+value)
	}

	assertSavedBudget()

	// Disabling caching preserves the saved preference for future use, but
	// maintenance must continue to work without compaction scratch files.
	e.RunAndExpectSuccess(t, "cache", "set", "--content-cache-size-mb=0")
	assertSavedBudget()

	out = e.RunAndExpectSuccess(t, "cache", "info")
	require.Contains(t, mustGetLineContaining(t, out, "Local caching is disabled"), "without a working-memory budget")
	e.RunAndExpectSuccess(t, "maintenance", "run", "--index-compaction-memory=1MB")
}
