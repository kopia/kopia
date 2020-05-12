package endtoend_test

import (
	"strings"
	"testing"

	"github.com/kylelemons/godebug/pretty"

	"github.com/kopia/kopia/tests/testenv"
)

func TestIndexRecover(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", ".")
	e.RunAndExpectSuccess(t, "snapshot", "list", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	sources := e.ListSnapshotsAndExpectSuccess(t)
	if got, want := len(sources), 3; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}

	contentsBefore := e.RunAndExpectSuccess(t, "content", "ls")

	lines := e.RunAndVerifyOutputLineCount(t, 6, "index", "ls")
	for _, l := range lines {
		indexFile := strings.Split(l, " ")[0]
		e.RunAndExpectSuccess(t, "blob", "delete", indexFile)
	}

	// clear the cache to get rid of cache of own writes.
	e.RunAndVerifyOutputLineCount(t, 0, "cache", "clear")

	// there should be no index files at this point
	e.RunAndVerifyOutputLineCount(t, 0, "index", "ls", "--no-list-caching")

	// there should be no contents, since there are no indexes to find them
	e.RunAndVerifyOutputLineCount(t, 0, "content", "ls")

	// now recover index from all blocks
	e.RunAndExpectSuccess(t, "index", "recover", "--commit")

	// all recovered index entries are added as index file
	e.RunAndVerifyOutputLineCount(t, 1, "index", "ls")

	contentsAfter := e.RunAndExpectSuccess(t, "content", "ls")
	if d := pretty.Compare(contentsBefore, contentsAfter); d != "" {
		t.Errorf("unexpected block diff after recovery: %v", d)
	}
}
