package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

// TestAutoMaintenanceOnlyAfterDataWrite is a regression test for #3174:
// opportunistic automatic maintenance must run only after data-modifying
// operations (e.g. snapshot create), not after control-only operations (e.g.
// policy set) that merely change repository configuration.
func TestAutoMaintenanceOnlyAfterDataWrite(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	readMaintenanceInfo := func() cli.MaintenanceInfo {
		var mi cli.MaintenanceInfo
		testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

		return mi
	}

	// Control-only operation: changes global policy, writes no snapshot data.
	// It must NOT trigger automatic maintenance.
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--keep-latest", "3")
	require.Empty(t, readMaintenanceInfo().Runs,
		"control-only 'policy set' must not trigger automatic maintenance")

	// Data-modifying operation: creates a snapshot. It SHOULD trigger
	// opportunistic automatic maintenance on success.
	srcdir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "some-file"), []byte{1, 2, 3}, 0o600))

	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)
	require.NotEmpty(t, readMaintenanceInfo().Runs,
		"data-modifying 'snapshot create' should trigger automatic maintenance")
}
