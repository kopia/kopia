package cli_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestCacheSet(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	// no changes
	env.RunAndExpectFailure(t, "cache", "set")

	ncd := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t,
		"cache", "set",
		"--cache-directory", ncd,
		"--max-list-cache-duration=55s",
	)

	env.RunAndExpectSuccess(t,
		"cache", "set",
		"--cache-directory", ncd,
		"--content-cache-size-mb=33",
		"--content-cache-size-limit-mb=331",
		"--metadata-cache-size-mb=44",
		"--metadata-cache-size-limit-mb=441",
	)

	out := env.RunAndExpectSuccess(t, "cache", "info")
	require.Contains(t, mustGetLineContaining(t, out, "33 MB"), ncd)
	require.Contains(t, mustGetLineContaining(t, out, "soft limit: 33 MB"), "contents")
	require.Contains(t, mustGetLineContaining(t, out, "hard limit: 331 MB"), "contents")
	require.Contains(t, mustGetLineContaining(t, out, "min sweep age: 10m0s"), "contents")

	require.Contains(t, mustGetLineContaining(t, out, "soft limit: 44 MB"), "metadata")
	require.Contains(t, mustGetLineContaining(t, out, "hard limit: 441 MB"), "metadata")
	require.Contains(t, mustGetLineContaining(t, out, "min sweep age: 24h0m0s"), "metadata")

	require.Contains(t, mustGetLineContaining(t, out, "55s"), "blob-list")
}

func mustGetLineContaining(t *testing.T, lines []string, containing string) string {
	t.Helper()

	for _, l := range lines {
		if strings.Contains(l, containing) {
			return l
		}
	}

	t.Fatalf("no line containing %q found in %v", containing, lines)

	return ""
}
