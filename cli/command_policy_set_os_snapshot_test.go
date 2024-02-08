package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSetOSSnapshotPolicy(t *testing.T) {
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	lines := e.RunAndExpectSuccess(t, "policy", "show", "--global")
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Volume Shadow Copy: never (defined for this target)")

	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--enable-volume-shadow-copy=when-available")

	lines = e.RunAndExpectSuccess(t, "policy", "show", "--global")
	lines = compressSpaces(lines)

	require.Contains(t, lines, " Volume Shadow Copy: when-available (defined for this target)")

	// make some directory we'll be setting policy on
	td := testutil.TempDirectory(t)

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)
	require.Contains(t, lines, " Volume Shadow Copy: when-available inherited from (global)")

	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--enable-volume-shadow-copy=always")

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)

	require.Contains(t, lines, " Volume Shadow Copy: always inherited from (global)")

	e.RunAndExpectSuccess(t, "policy", "set", "--enable-volume-shadow-copy=never", td)

	lines = e.RunAndExpectSuccess(t, "policy", "show", td)
	lines = compressSpaces(lines)

	require.Contains(t, lines, " Volume Shadow Copy: never (defined for this target)")
}
