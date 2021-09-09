package endtoend_test

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

// nolint:thelper
func (s *formatSpecificTestSuite) TestContentListAndStats(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	require.Empty(t, e.RunAndExpectSuccess(t, "content", "list", "--deleted-only"))
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--compression", "pgzip")

	srcDir := testutil.TempDirectory(t)
	ioutil.WriteFile(filepath.Join(srcDir, "compressible.txt"),
		bytes.Repeat([]byte{1, 2, 3, 4}, 1000),
		0o600,
	)

	var man snapshot.Manifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", srcDir, "--json"), &man)
	contentID := string(man.RootObjectID())

	require.True(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list"), contentID))
	require.True(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "-l"), contentID))
	require.True(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "-c"), contentID))
	require.True(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "--summary"), "Total: "))

	e.RunAndExpectSuccess(t, "content", "stats")

	// sleep a bit to ensure at least one second passes, otherwise delete may end up happen on the same
	// second as create, in which case creation will prevail.
	time.Sleep(time.Second)

	e.RunAndExpectSuccess(t, "content", "delete", contentID)

	require.False(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list"), contentID))
	require.False(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "-l"), contentID))
	require.False(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "-c"), contentID))

	require.True(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "--deleted"), contentID))
	require.True(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "--deleted", "-l"), contentID))
	require.True(t, containsLineStartingWith(e.RunAndExpectSuccess(t, "content", "list", "--deleted", "-c"), contentID))
}
