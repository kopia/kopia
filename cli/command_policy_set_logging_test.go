package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSetLoggingPolicy(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	lines := e.RunAndExpectSuccess(t, "policy", "show", "--global")
	lines = testutil.CompressSpaces(lines)
	require.Contains(t, lines, " Directory snapshotted: 5 (defined for this target)")
	require.Contains(t, lines, " Directory ignored: 5 (defined for this target)")
	require.Contains(t, lines, " Entry snapshotted: 0 (defined for this target)")
	require.Contains(t, lines, " Entry ignored: 5 (defined for this target)")
	require.Contains(t, lines, " Entry cache hit: 0 (defined for this target)")
	require.Contains(t, lines, " Entry cache miss: 0 (defined for this target)")

	// make some directory we'll be setting policy on
	td := testutil.TempDirectory(t)

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = testutil.CompressSpaces(lines)
	require.Contains(t, lines, " Directory snapshotted: 5 inherited from (global)")
	require.Contains(t, lines, " Directory ignored: 5 inherited from (global)")
	require.Contains(t, lines, " Entry snapshotted: 0 inherited from (global)")
	require.Contains(t, lines, " Entry ignored: 5 inherited from (global)")
	require.Contains(t, lines, " Entry cache hit: 0 inherited from (global)")
	require.Contains(t, lines, " Entry cache miss: 0 inherited from (global)")

	e.RunAndExpectSuccess(t, "policy", "set", td,
		"--log-dir-snapshotted=1",
		"--log-dir-ignored=2",
		"--log-entry-snapshotted=3",
		"--log-entry-ignored=4",
		"--log-entry-cache-hit=5",
		"--log-entry-cache-miss=6")

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = testutil.CompressSpaces(lines)
	require.Contains(t, lines, " Directory snapshotted: 1 (defined for this target)")
	require.Contains(t, lines, " Directory ignored: 2 (defined for this target)")
	require.Contains(t, lines, " Entry snapshotted: 3 (defined for this target)")
	require.Contains(t, lines, " Entry ignored: 4 (defined for this target)")
	require.Contains(t, lines, " Entry cache hit: 5 (defined for this target)")
	require.Contains(t, lines, " Entry cache miss: 6 (defined for this target)")

	e.RunAndExpectSuccess(t, "policy", "set", td, "--log-entry-ignored=inherit")

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = testutil.CompressSpaces(lines)
	require.Contains(t, lines, " Entry ignored: 5 inherited from (global)")

	e.RunAndExpectFailure(t, "policy", "set", td, "--log-dir-snapshotted=-1")
	e.RunAndExpectFailure(t, "policy", "set", td, "--log-dir-ignored=11")
	e.RunAndExpectFailure(t, "policy", "set", td, "--log-entry-snapshotted=xx")
	e.RunAndExpectFailure(t, "policy", "set", td, "--log-entry-ignored=-1")
	e.RunAndExpectFailure(t, "policy", "set", td, "--log-entry-cache-hit=-1")
	e.RunAndExpectFailure(t, "policy", "set", td, "--log-entry-cache-miss=-1")
}
