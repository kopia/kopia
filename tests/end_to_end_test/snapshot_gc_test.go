package endtoend_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestSnapshotGC(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	expectedContentCount := len(e.RunAndExpectSuccess(t, "content", "list"))

	dataDir := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(dataDir, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0o600))

	// take a snapshot of a directory with 1 file
	e.RunAndExpectSuccess(t, "snap", "create", dataDir)

	// data block + directory block + manifest block
	expectedContentCount += 3
	e.RunAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")

	// now delete all manifests, making the content unreachable
	for _, line := range e.RunAndExpectSuccess(t, "snap", "list", "-m") {
		p := strings.Index(line, "manifest:")
		if p >= 0 {
			manifestID := strings.TrimPrefix(strings.Split(line[p:], " ")[0], "manifest:")
			t.Logf("manifestID: %v", manifestID)
			e.RunAndExpectSuccess(t, "manifest", "rm", manifestID)
		}
	}

	// deletion of manifests creates a new manifest
	expectedContentCount++

	// run verification
	e.RunAndExpectSuccess(t, "snapshot", "verify")

	// run garbage collection through maintenance - this will not delete contents
	// because of default safety level which only looks at contents above certain age.
	e.RunAndExpectSuccess(t, "maintenance", "run", "--full", "--safety=full")

	// data block + directory block + manifest block + manifest block from manifest deletion
	var contentInfo []content.Info

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "content", "list", "--json"), &contentInfo)

	if got, want := len(contentInfo), expectedContentCount; got != want {
		t.Fatalf("unexpected number of contents: %v, want %v", got, want)
	}

	// make sure we are not too quick
	time.Sleep(2 * time.Second)

	// garbage-collect for real, this time without age limit
	e.RunAndExpectSuccess(t, "maintenance", "run", "--full", "--safety=none")

	// two contents are deleted
	expectedContentCount -= 2
	e.RunAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")
}
