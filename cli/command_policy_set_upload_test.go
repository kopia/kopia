package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSetUploadPolicy(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	lines := e.RunAndExpectSuccess(t, "policy", "show", "--global")
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Max parallel snapshots (server/UI): 1 (defined for this target)")
	require.Contains(t, lines, " Max parallel file reads: - (defined for this target)")
	require.Contains(t, lines, " Parallel upload above size: 2.1 GB (defined for this target)")

	// make some directory we'll be setting policy on
	td := testutil.TempDirectory(t)

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Max parallel snapshots (server/UI): 1 inherited from (global)")
	require.Contains(t, lines, " Max parallel file reads: - inherited from (global)")
	require.Contains(t, lines, " Parallel upload above size: 2.1 GB inherited from (global)")

	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--max-parallel-snapshots=7", "--max-parallel-file-reads=33", "--parallel-upload-above-size-mib=4096")

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)

	require.Contains(t, lines, " Max parallel snapshots (server/UI): 7 inherited from (global)")
	require.Contains(t, lines, " Max parallel file reads: 33 inherited from (global)")
	require.Contains(t, lines, " Parallel upload above size: 4.3 GB inherited from (global)")

	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--max-parallel-snapshots=default", "--max-parallel-file-reads=default", "--parallel-upload-above-size-mib=default")

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)

	require.Contains(t, lines, " Max parallel snapshots (server/UI): 1 inherited from (global)")
	require.Contains(t, lines, " Max parallel file reads: - inherited from (global)")
	require.Contains(t, lines, " Parallel upload above size: 2.1 GB inherited from (global)")
}
