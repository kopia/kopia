package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotCopy(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	e.PassthroughStderr = true

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=host1", "--override-username=user1")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	assertSnapshotCount(t, e, map[snapshot.SourceInfo]int{
		{Host: "host1", UserName: "user1", Path: sharedTestDataDir1}: 2,
	})

	// copy user1@host1 to user2@host1
	e.RunAndExpectSuccess(t, "snapshot", "copy-history", "user1@host1", "user2@host1")

	// copy user1@host1 to user2@host1
	assertSnapshotCount(t, e, map[snapshot.SourceInfo]int{
		{Host: "host1", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host1", UserName: "user2", Path: sharedTestDataDir1}: 2,
	})

	// copy @host1 to @host2
	e.RunAndExpectSuccess(t, "snapshot", "copy-history", "@host1", "@host2")
	assertSnapshotCount(t, e, map[snapshot.SourceInfo]int{
		{Host: "host1", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host1", UserName: "user2", Path: sharedTestDataDir1}: 2,
		{Host: "host2", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host2", UserName: "user2", Path: sharedTestDataDir1}: 2,
	})

	// move user1@host2 to user3@host3
	e.RunAndExpectSuccess(t, "snapshot", "move-history", "user1@host2", "user1@host3")
	assertSnapshotCount(t, e, map[snapshot.SourceInfo]int{
		{Host: "host1", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host1", UserName: "user2", Path: sharedTestDataDir1}: 2,
		{Host: "host3", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host2", UserName: "user2", Path: sharedTestDataDir1}: 2,
	})

	// move user1@host2 to @host4
	e.RunAndExpectSuccess(t, "snapshot", "move-history", "user1@host3", "@host4")
	assertSnapshotCount(t, e, map[snapshot.SourceInfo]int{
		{Host: "host1", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host1", UserName: "user2", Path: sharedTestDataDir1}: 2,
		{Host: "host4", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host2", UserName: "user2", Path: sharedTestDataDir1}: 2,
	})

	// copy user1@host1:sharedTestDataDir1 to @host5
	e.RunAndExpectSuccess(t, "snapshot", "copy-history", "user1@host1:"+sharedTestDataDir1, "@host5")
	assertSnapshotCount(t, e, map[snapshot.SourceInfo]int{
		{Host: "host1", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host1", UserName: "user2", Path: sharedTestDataDir1}: 2,
		{Host: "host4", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host2", UserName: "user2", Path: sharedTestDataDir1}: 2,
		{Host: "host5", UserName: "user1", Path: sharedTestDataDir1}: 2,
	})

	// copy user1@host1:sharedTestDataDir1 to another@host6:sharedTestDataDir2
	e.RunAndExpectSuccess(t, "snapshot", "copy-history", "user1@host1:"+sharedTestDataDir1, "user3@host6:"+sharedTestDataDir2)
	assertSnapshotCount(t, e, map[snapshot.SourceInfo]int{
		{Host: "host1", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host1", UserName: "user2", Path: sharedTestDataDir1}: 2,
		{Host: "host4", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host2", UserName: "user2", Path: sharedTestDataDir1}: 2,
		{Host: "host5", UserName: "user1", Path: sharedTestDataDir1}: 2,
		{Host: "host6", UserName: "user3", Path: sharedTestDataDir2}: 2,
	})
}

func assertSnapshotCount(t *testing.T, e *testenv.CLITest, wantSnapshotCounts map[snapshot.SourceInfo]int) {
	t.Helper()

	gotSnapshots := e.ListSnapshotsAndExpectSuccess(t, "-a")

	for si, wantCnt := range wantSnapshotCounts {
		found := false

		for _, v := range gotSnapshots {
			if v.Host == si.Host && v.User == si.UserName && v.Path == si.Path {
				found = true

				t.Logf("found %v snapshots for %v", len(v.Snapshots), si)

				if got, want := len(v.Snapshots), wantCnt; got != want {
					t.Fatalf("invalid number of snapshots for %v: %v, want %v", si, got, want)
				}
			}
		}

		if !found {
			t.Fatalf("snapshots not found for %v", si)
		}
	}

	if got, want := len(gotSnapshots), len(wantSnapshotCounts); got != want {
		t.Fatalf("unexpected number of sources: %v, want %v", got, want)
	}
}
