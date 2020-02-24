package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotCreate(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create some snapshots using different hostname/username
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir3)
	e.RunAndExpectSuccess(t, "snapshot", "list", sharedTestDataDir3)
	e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", ".")
	e.RunAndExpectSuccess(t, "snapshot", "list", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	sources := e.ListSnapshotsAndExpectSuccess(t)
	// will only list snapshots we created, not foo@foo
	if got, want := len(sources), 3; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}
}
