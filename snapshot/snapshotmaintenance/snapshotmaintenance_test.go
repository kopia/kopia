package snapshotmaintenance

import (
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot"
)

const (
	defaultPermissions = 0777
)

type testHarness struct {
	repotesting.Environment
	fakeTime  *faketime.TimeAdvance
	sourceDir *mockfs.Directory
}

func TestSnapshotGCSimple(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newTestHarness(t)

	require.NotNil(t, th)
	require.NotNil(t, th.sourceDir)
	th.sourceDir.AddDir("d1", defaultPermissions)
	th.sourceDir.AddFile("d1/f2", []byte{1, 2, 3, 4}, defaultPermissions)

	// Create and delete a snapshot of th.sourceDir dir, which contains 'd1'
	si := snapshot.SourceInfo{
		Host:     "host",
		UserName: "user",
		Path:     "/foo",
	}
	s1 := mustSnapshot(t, th.Repository, th.sourceDir, si)

	t.Log("snap 1:", pretty.Sprint(s1))
	mustFlush(t, th.Repository)

	// Delete snapshot
	err := th.Repository.Manifests.Delete(ctx, s1.ID)
	require.NoError(t, err)

	mustFlush(t, th.Repository)

	// Advance time to force GC to discard the contents of the previous snapshots
	th.fakeTime.Advance(maintenance.DefaultParams().SnapshotGC.MinContentAge + time.Hour)

	err = Run(ctx, th.Repository, maintenance.ModeFull, true)
	require.NoError(t, err)

	mustFlush(t, th.Repository)

	s2 := mustSnapshot(t, th.Repository, th.sourceDir, si)
	t.Log("snap 2:", pretty.Sprint(s2))
	mustFlush(t, th.Repository)

	info, err := th.Repository.Content.ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)

	t.Log("root info:", pretty.Sprint(info))
}

// Test maintenance when a directory is deleted and then reused.
// Scenario / events:
// - create snapshot s1 on a directory d is created
// - delete s1
// - let enough time pass so the contents in s1 are eligible for GC mark/deletion
// - concurrently create a snapshot s2 on directory d while performing full
//   maintenance
// - Check full maintenance can be run afterwards
// - Verify contents.
func TestMaintenanceReuseDirManifest(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newTestHarness(t)

	require.NotNil(t, th)
	require.NotNil(t, th.sourceDir)

	d1 := th.sourceDir.AddDir("d1", defaultPermissions)
	d1.AddFile("f1", []byte{1, 2, 3, 4}, defaultPermissions)

	// Create and delete a snapshot of th.sourceDir dir, which contains 'd1'
	si := snapshot.SourceInfo{
		Host:     "host",
		UserName: "user",
		Path:     "/foo",
	}
	s1 := mustSnapshot(t, th.Repository, th.sourceDir, si)

	t.Log("snap 1:", pretty.Sprint(s1))
	mustFlush(t, th.Repository)

	// Delete snapshot
	err := th.Repository.Manifests.Delete(ctx, s1.ID)
	require.NoError(t, err)

	mustFlush(t, th.Repository)

	// Advance time to force GC to discard the contents of the previous snapshots
	th.fakeTime.Advance(maintenance.DefaultParams().SnapshotGC.MinContentAge + time.Hour)

	r2 := th.openAnother(t)

	s2 := mustSnapshot(t, r2, th.sourceDir, si)
	t.Log("snap 2:", pretty.Sprint(s2))

	err = Run(ctx, th.Repository, maintenance.ModeFull, true)
	require.NoError(t, err)

	info, err := r2.(*repo.DirectRepository).Content.ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)
	require.False(t, info.Deleted, "content must not be deleted")

	_, err = r2.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	mustFlush(t, r2) // finish snapshot
	require.NoError(t, r2.Close(ctx))

	mustFlush(t, th.Repository) // finish maintenance

	th.MustReopen(t)

	info, err = th.Repository.Content.ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)
	require.True(t, info.Deleted, "content must be deleted")

	_, err = th.Repository.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	// Run maintenance again
	th.fakeTime.Advance(maintenance.DefaultParams().SnapshotGC.MinContentAge + time.Hour)
	err = Run(ctx, th.Repository, maintenance.ModeFull, true)
	require.NoError(t, err)
	mustFlush(t, th.Repository)

	// Was the previous root undeleted
	info, err = th.Repository.Content.ContentInfo(ctx, content.ID(s2.RootObjectID()))
	require.NoError(t, err)
	require.False(t, info.Deleted, "content must not be deleted")

	_, err = th.Repository.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	t.Log("root info:", pretty.Sprint(info))
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	baseTime := time.Date(2020, 9, 10, 0, 0, 0, 0, time.UTC)
	th := &testHarness{
		fakeTime:  faketime.NewTimeAdvance(baseTime, time.Second),
		sourceDir: mockfs.NewDirectory(),
	}

	th.Environment.Setup(t, repotesting.Options{OpenOptions: th.fakeTimeOpenRepoOption})

	require.NotNil(t, th.Repository)

	t.Cleanup(func() {
		th.Environment.Close(testlogging.Context(t), t)
	})

	return th
}

func (th *testHarness) fakeTimeOpenRepoOption(o *repo.Options) {
	o.TimeNowFunc = th.fakeTime.NowFunc()
}

func (th *testHarness) openAnother(t *testing.T) repo.Repository {
	r := th.MustConnectOpenAnother(t, th.fakeTimeOpenRepoOption)

	t.Cleanup(func() {
		r.Close(testlogging.Context(t))
	})

	return r
}

func mustFlush(t *testing.T, r repo.Repository) {
	t.Helper()
	require.NotNil(t, r, "nil repository")
	require.NoError(t, r.Flush(testlogging.Context(t)))
}

func mustSnapshot(t *testing.T, r repo.Repository, source fs.Entry, si snapshot.SourceInfo) *snapshot.Manifest {
	t.Helper()

	s1, err := createSnapshot(testlogging.Context(t), r, source, si, "")
	require.NoError(t, err)
	require.NotNil(t, s1)

	return s1
}
