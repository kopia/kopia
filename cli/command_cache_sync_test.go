package cli_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestCacheClearSync(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	emptyDir := testutil.TempDirectory(t)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectSuccess(t, "snapshot", "create", emptyDir)
	env.RunAndExpectSuccess(t, "snapshot", "create", emptyDir)
	env.RunAndExpectSuccess(t, "snapshot", "create", emptyDir)
	env.RunAndExpectSuccess(t, "snapshot", "create", emptyDir)
	env.RunAndExpectSuccess(t, "snapshot", "create", emptyDir)

	oldMetadataLine := mustGetLineContaining(t, env.RunAndExpectSuccess(t, "cache", "info"), "metadata")
	oldMetadataLine2 := mustGetLineContaining(t, env.RunAndExpectSuccess(t, "cache", "info"), "metadata")
	require.Equal(t, oldMetadataLine, oldMetadataLine2)

	env.RunAndExpectSuccess(t, "cache", "clear")

	newMetadataLine := mustGetLineContaining(t, env.RunAndExpectSuccess(t, "cache", "info"), "metadata")
	require.NotEqual(t, oldMetadataLine, newMetadataLine)

	env.RunAndExpectSuccess(t, "cache", "sync")
	newerMetadataLine := mustGetLineContaining(t, env.RunAndExpectSuccess(t, "cache", "info"), "metadata")

	env.RunAndExpectSuccess(t, "cache", "sync")
	newerMetadataLine2 := mustGetLineContaining(t, env.RunAndExpectSuccess(t, "cache", "info"), "metadata")

	require.NotEqual(t, newerMetadataLine, newMetadataLine)
	require.Equal(t, newerMetadataLine, newerMetadataLine2)
}
