package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSetSplitterPolicy(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	lines := e.RunAndExpectSuccess(t, "policy", "show", "--global")
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Algorithm override: (repository default) (defined for this target)")

	// make some directory we'll be setting policy on
	td := testutil.TempDirectory(t)

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Algorithm override: (repository default) inherited from (global)")

	e.RunAndExpectSuccess(t, "policy", "set", td, "--splitter=FIXED-4M")

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Algorithm override: FIXED-4M (defined for this target)")

	e.RunAndExpectSuccess(t, "policy", "set", td, "--splitter=inherit")

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Algorithm override: (repository default) inherited from (global)")

	e.RunAndExpectFailure(t, "policy", "set", td, "--splitter=NO-SUCH_SPLITTER")
}
