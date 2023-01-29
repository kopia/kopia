package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestMaintenanceSetExtendObjectLocks(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var mi cli.MaintenanceInfo

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.False(t, mi.ExtendObjectLocks, "ExtendOjectLocks should not default to enabled.")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--extend-object-locks", "true")

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.True(t, mi.ExtendObjectLocks, "ExtendOjectLocks should be enabled.")

	e.RunAndExpectSuccess(t, "maintenance", "set", "--extend-object-locks", "false")

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	require.False(t, mi.ExtendObjectLocks, "ExtendOjectLocks should be disabled.")
}
