package endtoend_test

import (
	"testing"
	"time"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

func TestFullMaintenance(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var snap snapshot.Manifest

	// after creation we'll have kopia.repository, 1 index + 1 pack blob
	if got, want := e.RunAndExpectSuccess(t, "blob", "list"), 3; len(got) != want {
		t.Fatalf("unexpected number of initial blobs: %v, want %v", got, want)
	}

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--json"), &snap)

	// avoid create and delete in the same second.
	time.Sleep(2 * time.Second)
	e.RunAndExpectSuccess(t, "snapshot", "delete", string(snap.ID), "--delete")

	e.RunAndVerifyOutputLineCount(t, 0, "snapshot", "list")

	originalBlobCount := len(e.RunAndExpectSuccess(t, "blob", "list"))

	e.RunAndVerifyOutputLineCount(t, 0, "maintenance", "run", "--full")

	if got := len(e.RunAndExpectSuccess(t, "blob", "list")); got != originalBlobCount {
		t.Fatalf("full maintenance is not expected to change any blobs due to safety margins (got %v, was %v)", got, originalBlobCount)
	}

	// now rerun with --safety=none
	e.RunAndExpectSuccess(t, "maintenance", "run", "--full", "--safety=none")

	if got := len(e.RunAndExpectSuccess(t, "blob", "list")); got >= originalBlobCount {
		t.Fatalf("maintenance did not remove blobs: %v, had %v", got, originalBlobCount)
	}

	// we're expecting to have 5 or 6 blobs:
	// - kopia.maintenance
	// - kopia.repository
	// - 2 index blobs
	// - 1 or 2 q blob

	const blobCountAfterFullWipeout = 6

	if got, want := e.RunAndExpectSuccess(t, "blob", "list"), blobCountAfterFullWipeout; len(got) > want {
		t.Fatalf("maintenance left unwanted blobs: %v, want %v", got, want)
	}
}
