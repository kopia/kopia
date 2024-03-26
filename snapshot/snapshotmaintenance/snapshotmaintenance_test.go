package snapshotmaintenance_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

const (
	defaultPermissions = 0o777
)

type testHarness struct {
	*repotesting.Environment
	fakeTime  *faketime.TimeAdvance
	sourceDir *mockfs.Directory
}

func (s *formatSpecificTestSuite) TestSnapshotGCSimple(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newTestHarness(t, s.formatVersion)

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
	s1 := mustSnapshot(t, th.RepositoryWriter, th.sourceDir, si)

	t.Log("snap 1:", pretty.Sprint(s1))
	mustFlush(t, th.RepositoryWriter)

	// Delete snapshot
	err := th.RepositoryWriter.DeleteManifest(ctx, s1.ID)
	require.NoError(t, err)

	mustFlush(t, th.RepositoryWriter)

	safety := maintenance.SafetyFull

	// Advance time to force GC to mark as deleted the contents from the previous snapshot
	th.fakeTime.Advance(safety.MinContentAgeSubjectToGC + time.Hour)

	err = snapshotmaintenance.Run(ctx, th.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull)
	require.NoError(t, err)

	mustFlush(t, th.RepositoryWriter)

	s2 := mustSnapshot(t, th.RepositoryWriter, th.sourceDir, si)
	t.Log("snap 2:", pretty.Sprint(s2))
	mustFlush(t, th.RepositoryWriter)

	info, err := th.RepositoryWriter.ContentInfo(ctx, mustGetContentID(t, s2.RootObjectID()))
	require.NoError(t, err)

	t.Log("root info:", pretty.Sprint(info))
}

// Test maintenance when a directory is deleted and then reused.
// Scenario / events:
//   - create snapshot s1 on a directory d is created
//   - delete s1
//   - let enough time pass so the contents in s1 are eligible for GC mark/deletion
//   - concurrently create a snapshot s2 on directory d while performing full
//     maintenance
//   - Check full maintenance can be run afterwards
//   - Verify contents.
func (s *formatSpecificTestSuite) TestMaintenanceReuseDirManifest(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newTestHarness(t, s.formatVersion)

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
	s1 := mustSnapshot(t, th.RepositoryWriter, th.sourceDir, si)

	t.Log("snap 1:", pretty.Sprint(s1))
	mustFlush(t, th.RepositoryWriter)

	// Delete snapshot
	err := th.RepositoryWriter.DeleteManifest(ctx, s1.ID)
	require.NoError(t, err)

	mustFlush(t, th.RepositoryWriter)

	safety := maintenance.SafetyFull

	// Advance time to force GC to mark as deleted the contents from the previous snapshot
	th.fakeTime.Advance(safety.MinContentAgeSubjectToGC + time.Hour)

	r2 := th.openAnother(t)

	s2 := mustSnapshot(t, r2, th.sourceDir, si)
	t.Log("snap 2:", pretty.Sprint(s2))

	// interleaving snapshot and maintenance and delaying flushing as well to
	// create dangling references to contents that were in the previously
	// deleted snapshot and that are reused in this new snapshot.
	err = snapshotmaintenance.Run(ctx, th.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull)
	require.NoError(t, err)

	info, err := r2.(repo.DirectRepository).ContentInfo(ctx, mustGetContentID(t, s2.RootObjectID()))
	require.NoError(t, err)
	require.False(t, info.Deleted, "content must not be deleted")

	_, err = r2.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	mustFlush(t, r2) // finish snapshot
	require.NoError(t, r2.Close(ctx))

	mustFlush(t, th.RepositoryWriter) // finish maintenance

	th.MustReopen(t, th.fakeTimeOpenRepoOption)

	info, err = th.RepositoryWriter.ContentInfo(ctx, mustGetContentID(t, s2.RootObjectID()))
	require.NoError(t, err)
	require.True(t, info.Deleted, "content must be deleted")

	_, err = th.RepositoryWriter.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	// Run maintenance again
	th.fakeTime.Advance(safety.MinContentAgeSubjectToGC + time.Hour)
	err = snapshotmaintenance.Run(ctx, th.RepositoryWriter, maintenance.ModeFull, true, safety)
	require.NoError(t, err)
	mustFlush(t, th.RepositoryWriter)

	// Was the previous root undeleted
	info, err = th.RepositoryWriter.ContentInfo(ctx, mustGetContentID(t, s2.RootObjectID()))
	require.NoError(t, err)
	require.False(t, info.Deleted, "content must not be deleted")

	_, err = th.RepositoryWriter.VerifyObject(ctx, s2.RootObjectID())
	require.NoError(t, err)

	t.Log("root info:", pretty.Sprint(info))
}

func (s *formatSpecificTestSuite) TestSnapshotGCMinContentAgeSafety(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newTestHarness(t, s.formatVersion)

	require.NotNil(t, th)
	require.NotNil(t, th.sourceDir)
	th.sourceDir.AddFile("f2", []byte{1, 2, 3, 4}, defaultPermissions)

	// Create and delete a snapshot of th.sourceDir dir, which contains 'd1'
	si := snapshot.SourceInfo{
		Host:     "host",
		UserName: "user",
		Path:     "/foo",
	}

	mustSnapshot(t, th.RepositoryWriter, th.sourceDir, si)
	mustFlush(t, th.RepositoryWriter)

	const contentCount = 10000

	require.NoError(t, th.Repository.Refresh(ctx))

	// create 10000 unreferenced contents
	oids := create4ByteObjects(t, th.Repository, 0, contentCount)

	require.NoError(t, th.Repository.Refresh(ctx))

	cids := objectIDsToContentIDs(t, oids)

	// check contents are not marked as deleted
	checkContentDeletion(t, th.Repository, cids, false)

	mustFlush(t, th.RepositoryWriter)

	safety := maintenance.SafetyFull

	// Advance time so the first content created above is close fall of the
	// SafetyFull.MinContentAgeSubjectToGC window. Leave a small buffer
	// of 20 seconds below for "passage of time" between now an when snapshot
	// GC actually starts.
	// Note: The duration of this buffer is a "magical number" that depends on
	// the number of times time is advanced between "now" and when snapshot
	// gc start
	ci, err := th.Repository.ContentInfo(ctx, cids[0])
	require.NoError(t, err)
	require.NotEmpty(t, ci)

	timeAdvance := safety.MinContentAgeSubjectToGC - th.fakeTime.NowFunc()().Sub(ci.Timestamp()) - 20*time.Second

	require.Positive(t, timeAdvance)
	th.fakeTime.Advance(timeAdvance)

	err = snapshotmaintenance.Run(ctx, th.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull)
	require.NoError(t, err)

	mustFlush(t, th.RepositoryWriter)

	// check contents have not been marked as deleted.
	checkContentDeletion(t, th.Repository, cids, false)
}

func newTestHarness(t *testing.T, formatVersion format.Version) *testHarness {
	t.Helper()

	baseTime := time.Date(2020, 9, 10, 0, 0, 0, 0, time.UTC)
	th := &testHarness{
		fakeTime:  faketime.NewAutoAdvance(baseTime, time.Second),
		sourceDir: mockfs.NewDirectory(),
	}

	_, th.Environment = repotesting.NewEnvironment(t, formatVersion, repotesting.Options{OpenOptions: th.fakeTimeOpenRepoOption})

	require.NotNil(t, th.RepositoryWriter)

	return th
}

func (s *formatSpecificTestSuite) TestMaintenanceAutoLiveness(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Version = format.FormatVersion1
		},
	})

	// create dummy snapshot.
	si := snapshot.SourceInfo{
		Host:     "host",
		UserName: "user",
		Path:     "/foo",
	}

	dir := mockfs.NewDirectory()
	dir.AddDir("d1", defaultPermissions)
	dir.AddFile("d1/f2", []byte{1, 2, 3, 4}, defaultPermissions)

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := createSnapshot(testlogging.Context(t), w, dir, si, "")
		if err != nil {
			return errors.Wrap(err, "unable to create snapshot")
		}

		dp := maintenance.DefaultParams()
		dp.Owner = env.Repository.ClientOptions().UsernameAtHost()
		return maintenance.SetParams(ctx, w, &dp)
	}))

	// simulate several weeks of triggering auto maintenance few times an hour.
	deadline := ft.NowFunc()().Add(21 * 24 * time.Hour)

	for ft.NowFunc()().Before(deadline) {
		ft.Advance(30 * time.Minute)

		t.Logf("running maintenance at %v", ft.NowFunc()())
		require.NoError(t, repo.DirectWriteSession(ctx, env.RepositoryWriter, repo.WriteSessionOptions{}, func(ctx context.Context, dw repo.DirectRepositoryWriter) error {
			return snapshotmaintenance.Run(context.Background(), dw, maintenance.ModeAuto, false, maintenance.SafetyFull)
		}))

		// verify that at all points in time the last execution time of all tasks is in the last 48 hours.
		const maxTimeSinceLastRun = 48 * time.Hour

		sched, err := maintenance.GetSchedule(ctx, env.RepositoryWriter)
		require.NoError(t, err)

		now := ft.NowFunc()()

		for k, v := range sched.Runs {
			if age := now.Sub(v[0].End); age > maxTimeSinceLastRun {
				if age > maxTimeSinceLastRun {
					t.Fatalf("at %v the last run of %v was too old (%v vs %v)", now, k, age, maxTimeSinceLastRun)
				}
			}
		}
	}

	// make sure all tasks executed at least once.
	sched, err := maintenance.GetSchedule(ctx, env.RepositoryWriter)
	require.NoError(t, err)

	require.NotEmpty(t, sched.Runs[maintenance.TaskDeleteOrphanedBlobsFull], maintenance.TaskDeleteOrphanedBlobsFull)
	require.NotEmpty(t, sched.Runs[maintenance.TaskDeleteOrphanedBlobsQuick], maintenance.TaskDeleteOrphanedBlobsQuick)
	require.NotEmpty(t, sched.Runs[maintenance.TaskDropDeletedContentsFull], maintenance.TaskDropDeletedContentsFull)
	require.NotEmpty(t, sched.Runs[maintenance.TaskIndexCompaction], maintenance.TaskIndexCompaction)
	require.NotEmpty(t, sched.Runs[maintenance.TaskRewriteContentsFull], maintenance.TaskRewriteContentsFull)
	require.NotEmpty(t, sched.Runs[maintenance.TaskRewriteContentsQuick], maintenance.TaskRewriteContentsQuick)
	require.NotEmpty(t, sched.Runs[maintenance.TaskSnapshotGarbageCollection], maintenance.TaskSnapshotGarbageCollection)
}

func (th *testHarness) fakeTimeOpenRepoOption(o *repo.Options) {
	o.TimeNowFunc = th.fakeTime.NowFunc()
}

func (th *testHarness) openAnother(t *testing.T) repo.RepositoryWriter {
	t.Helper()

	r := th.MustConnectOpenAnother(t, th.fakeTimeOpenRepoOption)
	ctx := testlogging.Context(t)

	t.Cleanup(func() {
		r.Close(ctx)
	})

	_, w, err := r.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	require.NoError(t, err)

	return w
}

func mustFlush(t *testing.T, r repo.RepositoryWriter) {
	t.Helper()
	require.NotNil(t, r, "nil repository")
	require.NoError(t, r.Flush(testlogging.Context(t)))
}

func mustSnapshot(t *testing.T, r repo.RepositoryWriter, source fs.Entry, si snapshot.SourceInfo) *snapshot.Manifest {
	t.Helper()

	s1, err := createSnapshot(testlogging.Context(t), r, source, si, "")
	require.NoError(t, err)
	require.NotNil(t, s1)

	return s1
}

func mustGetContentID(t *testing.T, oid object.ID) content.ID {
	t.Helper()

	c, _, ok := oid.ContentID()
	require.True(t, ok)

	return c
}

func create4ByteObjects(t *testing.T, r repo.Repository, base, count int) []object.ID {
	t.Helper()

	oids := make([]object.ID, 0, count)
	ctx := testlogging.Context(t)

	ctx, rw, err := r.NewWriter(ctx, repo.WriteSessionOptions{})
	require.NoError(t, err)

	defer rw.Close(ctx)

	var b [4]byte

	for i := base; i < base+count; i++ {
		w := rw.NewObjectWriter(ctx, object.WriterOptions{Description: "create-test-contents"})

		binary.BigEndian.PutUint32(b[:], uint32(i))

		_, err := w.Write(b[:])
		require.NoError(t, err)

		oid, err := w.Result()
		require.NoError(t, err)

		require.NoError(t, w.Close())

		oids = append(oids, oid)
	}

	require.NoError(t, rw.Flush(ctx))

	return oids
}

func objectIDsToContentIDs(t *testing.T, oids []object.ID) []content.ID {
	t.Helper()

	cids := make([]content.ID, 0, len(oids))

	for _, oid := range oids {
		cid, _, ok := oid.ContentID()

		require.True(t, ok)

		cids = append(cids, cid)
	}

	return cids
}

func checkContentDeletion(t *testing.T, r repo.Repository, cids []content.ID, deleted bool) {
	t.Helper()

	ctx := testlogging.Context(t)

	for i, cid := range cids {
		ci, err := r.ContentInfo(ctx, cid)

		require.NoErrorf(t, err, "i:%d cid:%s", i, cid)
		require.Equalf(t, deleted, ci.Deleted, "i:%d cid:%s", i, cid)
	}
}
