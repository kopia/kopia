package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestRepositorySync(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	sources := e.ListSnapshotsAndExpectSuccess(t)

	// synchronize repository blobs to another directory
	dir2 := makeScratchDir(t)
	e.RunAndExpectSuccess(t, "repo", "sync-to", "filesystem", "--path", dir2)

	// synchronizing to empty directory fails with --must-exist
	dir3 := makeScratchDir(t)
	e.RunAndExpectFailure(t, "repo", "sync-to", "filesystem", "--path", dir3, "--must-exist")

	// now connect to the new repository in new location
	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", dir2)

	// snapshot list should be the same
	sources2 := e.ListSnapshotsAndExpectSuccess(t)
	if got, want := len(sources2), len(sources); got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources2)
	}

	// now create a whole new repository
	e2 := testenv.NewCLITest(t)
	defer e2.Cleanup(t)
	defer e2.RunAndExpectSuccess(t, "repo", "disconnect")

	e2.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e2.RepoDir)

	// syncing to the directory should fail because it contains incompatible format blob.
	e2.RunAndExpectFailure(t, "repo", "sync-to", "filesystem", "--path", dir2)
}
