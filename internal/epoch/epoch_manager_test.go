package epoch

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/fault"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/blob/readonly"
)

type fakeIndex struct {
	Entries []int `json:"entries"`
}

func (n *fakeIndex) Bytes() []byte {
	v, err := json.Marshal(n)
	if err != nil {
		panic("err: " + err.Error())
	}

	return v
}

func parseFakeIndex(b []byte) (*fakeIndex, error) {
	r := &fakeIndex{}
	err := json.Unmarshal(b, &r)

	return r, errors.Wrap(err, "error unmashaling JSON")
}

func newFakeIndexWithEntries(entries ...int) *fakeIndex {
	return &fakeIndex{
		Entries: entries,
	}
}

type epochManagerTestEnv struct {
	data          blobtesting.DataMap
	unloggedst    blob.Storage
	st            blob.Storage
	ft            *faketime.ClockTimeWithOffset
	mgr           *Manager
	faultyStorage *blobtesting.FaultyStorage
}

func (te *epochManagerTestEnv) compact(ctx context.Context, blobs []blob.ID, prefix blob.ID) error {
	merged, err := te.getMergedIndexContents(ctx, blobs)
	if err != nil {
		return errors.Wrap(err, "unable to merge")
	}

	return errors.Wrap(
		te.st.PutBlob(ctx, blob.ID(fmt.Sprintf("%v%016x-s0-c1", prefix, rand.Int63())), gather.FromSlice(merged.Bytes()), blob.PutOptions{}),
		"PutBlob error")
}

// write two dummy compaction blobs instead of 3, simulating a compaction that crashed before fully complete.
func (te *epochManagerTestEnv) interruptedCompaction(ctx context.Context, _ []blob.ID, prefix blob.ID) error {
	sess := rand.Int63()

	te.st.PutBlob(ctx, blob.ID(fmt.Sprintf("%v%016x-s%v-c3", prefix, sess, rand.Int63())), gather.FromSlice([]byte("dummy")), blob.PutOptions{})
	te.st.PutBlob(ctx, blob.ID(fmt.Sprintf("%v%016x-s%v-c3", prefix, sess, rand.Int63())), gather.FromSlice([]byte("dummy")), blob.PutOptions{})

	return errors.New("failed for some reason")
}

func newTestEnv(t *testing.T) *epochManagerTestEnv {
	t.Helper()

	data := blobtesting.DataMap{}
	ft := faketime.NewClockTimeWithOffset(0)
	ms := blobtesting.NewMapStorage(data, nil, ft.NowFunc())
	fs := blobtesting.NewFaultyStorage(ms)
	st := logging.NewWrapper(fs, testlogging.NewTestLogger(t), "[STORAGE] ")
	te := &epochManagerTestEnv{unloggedst: ms, st: st, ft: ft}
	m := NewManager(te.st, parameterProvider{&Parameters{
		Enabled:                 true,
		EpochRefreshFrequency:   20 * time.Minute,
		FullCheckpointFrequency: 7,
		// increased safety margin because we're moving fake clock very fast
		CleanupSafetyMargin:                   48 * time.Hour,
		MinEpochDuration:                      12 * time.Hour,
		EpochAdvanceOnCountThreshold:          15,
		EpochAdvanceOnTotalSizeBytesThreshold: 20 << 20,
		DeleteParallelism:                     1,
	}}, te.compact, testlogging.NewTestLogger(t), te.ft.NowFunc())
	te.mgr = m
	te.faultyStorage = fs
	te.data = data

	t.Cleanup(te.mgr.Flush)

	return te
}

func (te *epochManagerTestEnv) another() *epochManagerTestEnv {
	te2 := &epochManagerTestEnv{
		data:          te.data,
		unloggedst:    te.unloggedst,
		st:            te.st,
		ft:            te.ft,
		faultyStorage: te.faultyStorage,
	}

	te2.mgr = NewManager(te2.st, te.mgr.paramProvider, te2.compact, te.mgr.log, te.mgr.timeFunc)

	return te2
}

func TestIndexEpochManager_Regular(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	verifySequentialWrites(t, te)
}

func TestIndexEpochManager_Parallel(t *testing.T) {
	t.Parallel()
	testutil.SkipNonDeterministicTestUnderCodeCoverage(t)

	if testing.Short() {
		return
	}

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	eg, ctx := errgroup.WithContext(ctx)

	// run for 30 seconds of real time or 60 days of fake time which advances much faster
	endFakeTime := te.ft.NowFunc()().Add(60 * 24 * time.Hour)
	endTimeReal := clock.Now().Add(30 * time.Second)

	for worker := 1; worker <= 5; worker++ {
		te2 := te.another()
		indexNum := 1e6 * worker

		eg.Go(func() error {
			_ = te2

			var (
				previousEntries      []int
				writtenEntries       []int
				blobNotFoundCount    int
				successfulMergeCount int
			)

			for te2.ft.NowFunc()().Before(endFakeTime) && clock.Now().Before(endTimeReal) {
				if err := ctx.Err(); err != nil {
					return err
				}

				indexNum++

				rnd := rand.Uint64()
				ndx := newFakeIndexWithEntries(indexNum)

				if _, err := te2.mgr.WriteIndex(ctx, map[blob.ID]blob.Bytes{
					blob.ID(fmt.Sprintf("w%vr%0x", worker, rnd)): gather.FromSlice(ndx.Bytes()),
				}); err != nil {
					if errors.Is(err, ErrVerySlowIndexWrite) {
						indexNum--
						continue
					}

					return errors.Wrap(err, "error writing")
				}

				writtenEntries = append(writtenEntries, indexNum)

				blobs, _, err := te2.mgr.GetCompleteIndexSet(ctx, LatestEpoch)
				if err != nil {
					return errors.Wrap(err, "GetCompleteIndexSet")
				}

				merged, err := te2.getMergedIndexContents(ctx, blob.IDsFromMetadata(blobs))
				if err != nil {
					if errors.Is(err, blob.ErrBlobNotFound) {
						// ErrBlobNotFound is unavoidable because another thread may decide
						// to delete some blobs after we compute the index set.
						blobNotFoundCount++
						continue
					}

					return errors.Wrap(err, "getMergedIndexContents")
				}

				successfulMergeCount++

				if err := verifySuperset(previousEntries, merged.Entries); err != nil {
					return errors.Wrap(err, "verifySuperset")
				}

				if err := verifySuperset(writtenEntries, merged.Entries); err != nil {
					return errors.Wrap(err, "verifySuperset")
				}

				previousEntries = merged.Entries

				dt := randomTime(1*time.Minute, 3*time.Hour)
				te2.ft.Advance(dt)

				time.Sleep(100 * time.Millisecond)
			}

			// allow for 5% of NOT_FOUND races
			if float64(blobNotFoundCount)/float64(successfulMergeCount) > 0.05 {
				t.Fatalf("too many not found cases")
			}

			t.Logf("worker %v wrote %v", worker, indexNum)

			return nil
		})
	}

	require.NoError(t, eg.Wait())
}

// verifySuperset verifies that every element in 'a' is also found in 'b'.
// Both sets are sorted and unique.
func verifySuperset(a, b []int) error {
	nextB := 0

	for _, it := range a {
		for nextB < len(b) && b[nextB] < it {
			nextB++
		}

		if nextB >= len(b) || b[nextB] != it {
			return errors.Errorf("%v not found", it)
		}
	}

	return nil
}

func TestIndexEpochManager_RogueBlobs(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	te.data[EpochMarkerIndexBlobPrefix+"zzzz"] = []byte{1}
	te.data[SingleEpochCompactionBlobPrefix+"zzzz"] = []byte{1}
	te.data[RangeCheckpointIndexBlobPrefix+"zzzz"] = []byte{1}
	te.data[DeletionWatermarkBlobPrefix+"zzzz"] = []byte{1}

	verifySequentialWrites(t, te)
	te.mgr.CleanupSupersededIndexes(testlogging.Context(t))
}

func TestIndexEpochManager_CompactionSilentlyDoesNothing(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	// set up test environment in which compactions never succeed for whatever reason.
	te.mgr.compact = func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
		return nil
	}

	verifySequentialWrites(t, te)
}

func TestIndexEpochManager_CompactionAlwaysFails(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	// set up test environment in which compactions never succeed for whatever reason.
	te.mgr.compact = func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
		return errors.New("testing compaction error")
	}

	verifySequentialWrites(t, te)
}

func TestIndexEpochManager_CompactionRandomlyCrashed(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	// set up test environment in which compactions never succeed for whatever reason.
	te.mgr.compact = func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
		if rand.Intn(100) < 20 {
			return te.interruptedCompaction(ctx, blobIDs, outputPrefix)
		}

		return te.compact(ctx, blobIDs, outputPrefix)
	}

	verifySequentialWrites(t, te)
}

func TestIndexEpochManager_DeletionFailing(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	te.faultyStorage.
		AddFault(blobtesting.MethodDeleteBlob).
		ErrorInstead(errors.New("something bad happened")).
		Repeat(200)

	// set up test environment in which compactions never succeed for whatever reason.
	te.mgr.compact = func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
		if rand.Intn(100) < 5 {
			return te.interruptedCompaction(ctx, blobIDs, outputPrefix)
		}

		return te.compact(ctx, blobIDs, outputPrefix)
	}

	verifySequentialWrites(t, te)
}

func TestIndexEpochManager_NoCompactionInReadOnly(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	te := newTestEnv(t)

	// Disable compaction so the other instance of the manager will try to compact
	// things. Unfortunately we can't check directly for compaction errors in our
	// read-only instance though.
	te.mgr.compact = func(context.Context, []blob.ID, blob.ID) error {
		return nil
	}

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	// Write data to the index such that the next time it's opened it should
	// attempt to compact things and advance the epoch. We want to write exactly
	// the number of blobs that will cause it to advance so we can keep track of
	// which epoch we're on and everything.
	for range 10 {
		for i := range p.GetEpochAdvanceOnCountThreshold() {
			// Advance the time so that the difference in times for writes will force
			// new epochs.
			te.ft.Advance(48 * time.Hour)
			te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(i))
		}
	}

	te.mgr.Flush()

	// Delete the final epoch marker so that te2 attempts to make a new one on
	// the refresh below. This simulates the previous epoch manager exiting (e.x.
	// crashing) before writing the new marker.
	c, err := te.mgr.Current(ctx)
	require.NoError(t, err, "getting current epoch")

	te.st.DeleteBlob(ctx, blob.ID(fmt.Sprintf("%s%d", string(EpochMarkerIndexBlobPrefix), c.WriteEpoch+1)))

	st := readonly.NewWrapper(te.unloggedst)
	fs := blobtesting.NewFaultyStorage(st)

	te2 := &epochManagerTestEnv{
		data:          te.data,
		unloggedst:    st,
		st:            logging.NewWrapper(fs, testlogging.NewTestLogger(t), "[OTHER STORAGE] "),
		ft:            te.ft,
		faultyStorage: fs,
	}

	// Set new epoch manager to read-only to ensure we don't get stuck.
	te2.mgr = NewManager(te2.st, te.mgr.paramProvider, te2.compact, te.mgr.log, te.mgr.timeFunc)

	// Use assert.Eventually here so we'll exit the test early instead of getting
	// stuck until the timeout.
	loadedDone := &atomic.Bool{}

	var loadedErr atomic.Value

	go func() {
		if err := te2.mgr.Refresh(ctx); err != nil {
			loadedErr.Store(err)
		}

		te2.mgr.backgroundWork.Wait()
		loadedDone.Store(true)
	}()

	require.Eventually(t, loadedDone.Load, time.Second*2, time.Second)

	assert.Nil(t, loadedErr.Load(), "refreshing read-only index")
}

func TestNoEpochAdvanceOnIndexRead(t *testing.T) {
	const epochs = 3

	t.Parallel()

	ctx := testlogging.Context(t)
	te := newTestEnv(t)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	count := p.GetEpochAdvanceOnCountThreshold()
	minDuration := p.MinEpochDuration

	cs, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, cs.WriteEpoch, "write epoch mismatch")

	// Write enough index blobs such that the next time the manager loads
	// indexes it should attempt to advance the epoch.
	// Write exactly the number of index blobs that will cause it to advance so
	// we can keep track of which one is the current epoch.
	for range epochs {
		for i := range count - 1 {
			te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(i))
		}

		te.ft.Advance(3*minDuration + time.Second)
		te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(count-1))
		// this could advance the epoch on write
		te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(count-1))
	}

	te.mgr.Invalidate()
	cs, err = te.mgr.Current(ctx)
	require.NoError(t, err)

	te.mgr.Flush() // wait for background work

	// get written lastWriteEpoch markers if any
	var (
		lastWriteEpoch int
		epochMarkers   []blob.ID
		deletedMarker  blob.ID
	)

	te.st.ListBlobs(ctx, EpochMarkerIndexBlobPrefix, func(bm blob.Metadata) error {
		epochMarkers = append(epochMarkers, bm.BlobID)

		return nil
	})

	t.Log("epoch marker blobs:", epochMarkers)

	if emLen := len(epochMarkers); emLen > 0 {
		var ok bool // to prevent shadowing 'lastWriteEpoch' below

		deletedMarker = epochMarkers[emLen-1]
		lastWriteEpoch, ok = epochNumberFromBlobID(deletedMarker)

		require.True(t, ok, "could not parse epoch from marker blob")
	}

	require.Equal(t, 0, lastWriteEpoch, "epoch should NOT have advanced")

	// reload indexes
	te.mgr.Invalidate()

	cs, err = te.mgr.Current(ctx)
	require.NoError(t, err)

	// wait for any background work, there shouldn't be any
	te.mgr.backgroundWork.Wait()

	require.Equal(t, 0, cs.WriteEpoch, "epoch should NOT have advanced")

	te.st.ListBlobs(ctx, EpochMarkerIndexBlobPrefix, func(bm blob.Metadata) error {
		t.Fatal("deleted epoch marker should NOT be found in the store:", deletedMarker)

		return nil
	})
}

func TestRefreshRetriesIfTakingTooLong(t *testing.T) {
	te := newTestEnv(t)

	te.faultyStorage.AddFault(blobtesting.MethodListBlobs).
		Repeat(8). // refresh does 7 lists, so this will cause 2 unsuccessful retries
		Before(func() { te.ft.Advance(24 * time.Hour) })

	ctx := testlogging.Context(t)

	require.NoError(t, te.mgr.Refresh(ctx))

	require.EqualValues(t, 2, *te.mgr.committedStateRefreshTooSlow)
}

func TestGetCompleteIndexSetRetriesIfTookTooLong(t *testing.T) {
	te := newTestEnv(t)

	ctx := testlogging.Context(t)

	// advance by 3 epochs to ensure GetCompleteIndexSet will be trying to list
	// some blobs that were not fetched during Refresh().
	te.mgr.forceAdvanceEpoch(ctx)
	te.ft.Advance(1 * time.Hour)
	te.mgr.forceAdvanceEpoch(ctx)
	te.ft.Advance(1 * time.Hour)
	te.mgr.forceAdvanceEpoch(ctx)
	te.ft.Advance(1 * time.Hour)

	// load committed state
	require.NoError(t, te.mgr.Refresh(ctx))

	cnt := new(int32)

	// ensure we're not running any background goroutines before modifying 'Faults'
	te.mgr.Flush()

	te.faultyStorage.AddFault(blobtesting.MethodListBlobs).
		Repeat(1000).
		Before(func() {
			if atomic.AddInt32(cnt, 1) == 1 {
				te.ft.Advance(24 * time.Hour)
			}
		})

	_, _, err := te.mgr.GetCompleteIndexSet(ctx, 0)
	require.NoError(t, err)

	require.EqualValues(t, 1, *te.mgr.getCompleteIndexSetTooSlow)
}

func TestSlowWrite_RefreshesCurrentState(t *testing.T) {
	te := newTestEnv(t)

	ctx := testlogging.Context(t)

	// on first write, advance time enough to lose current context, but not to go to the next epoch.
	te.faultyStorage.AddFaults(blobtesting.MethodPutBlob,
		fault.New().Before(func() { te.ft.Advance(1 * time.Hour) }))

	te.mustWriteIndexFiles(ctx, t,
		newFakeIndexWithEntries(1),
		newFakeIndexWithEntries(2),
		newFakeIndexWithEntries(3),
	)
	require.EqualValues(t, 1, *te.mgr.writeIndexTooSlow)
	cs, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, cs.WriteEpoch)
	te.verifyCompleteIndexSet(ctx, t, LatestEpoch, newFakeIndexWithEntries(1, 2, 3), time.Time{})
}

func TestSlowWrite_MovesToNextEpoch(t *testing.T) {
	te := newTestEnv(t)

	ctx := testlogging.Context(t)

	// on first write, advance time enough to lose current context and go to the next epoch.
	te.faultyStorage.AddFaults(blobtesting.MethodPutBlob,
		fault.New().Before(func() {
			te.ft.Advance(1 * time.Hour)
			te.mgr.forceAdvanceEpoch(ctx)
		}),
		fault.New().Before(func() { te.ft.Advance(1 * time.Hour) }))

	te.mustWriteIndexFiles(ctx, t,
		newFakeIndexWithEntries(1),
		newFakeIndexWithEntries(2),
		newFakeIndexWithEntries(3),
	)
	require.EqualValues(t, 1, *te.mgr.writeIndexTooSlow)
	cs, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, cs.WriteEpoch)
	te.verifyCompleteIndexSet(ctx, t, LatestEpoch, newFakeIndexWithEntries(1, 2, 3), time.Time{})
}

func TestSlowWrite_MovesToNextEpochTwice(t *testing.T) {
	te := newTestEnv(t)

	ctx := testlogging.Context(t)

	// on first write, advance time enough to lose current context and go to the next epoch.
	te.faultyStorage.AddFaults(blobtesting.MethodPutBlob,
		fault.New().Before(func() {
			te.ft.Advance(24 * time.Hour)
		}),
		fault.New().Before(func() {
			te.mgr.forceAdvanceEpoch(ctx)
			te.mgr.forceAdvanceEpoch(ctx)
		}))

	_, err := te.writeIndexFiles(ctx,
		newFakeIndexWithEntries(1),
		newFakeIndexWithEntries(2),
		newFakeIndexWithEntries(3),
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "slow index write")
}

func TestMaybeAdvanceEpoch_Empty(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	te.verifyCurrentWriteEpoch(t, 0)

	// this should be a no-op
	err := te.mgr.MaybeAdvanceWriteEpoch(ctx)

	require.NoError(t, err)

	// check current epoch again
	te.verifyCurrentWriteEpoch(t, 0)
}

func TestMaybeAdvanceEpoch(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	te := newTestEnv(t)

	// Disable automatic epoch advancement and compaction to build up state
	te.mgr.compact = func(context.Context, []blob.ID, blob.ID) error {
		return nil
	}

	te.verifyCurrentWriteEpoch(t, 0)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	idxCount := p.GetEpochAdvanceOnCountThreshold()
	// Create sufficient indexes blobs and move clock forward to advance epoch.
	for i := range idxCount {
		te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(i))
	}

	te.verifyCurrentWriteEpoch(t, 0)

	// Advance the time so that the difference in times for writes will force
	// new epochs.
	te.ft.Advance(p.MinEpochDuration + 1*time.Hour)

	err = te.mgr.Refresh(ctx)
	require.NoError(t, err)

	te.verifyCurrentWriteEpoch(t, 0)

	// one more to go over the threshold
	te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(idxCount))
	err = te.mgr.Refresh(ctx)

	require.NoError(t, err)
	te.verifyCurrentWriteEpoch(t, 0)

	err = te.mgr.MaybeAdvanceWriteEpoch(ctx)

	require.NoError(t, err)

	err = te.mgr.Refresh(ctx) // force state refresh

	require.NoError(t, err)
	te.verifyCurrentWriteEpoch(t, 1)
}

type faultyParamsProvider struct {
	err error
}

func (p faultyParamsProvider) GetParameters(ctx context.Context) (*Parameters, error) {
	return nil, p.err
}

func TestMaybeAdvanceEpoch_GetParametersError(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	paramsError := errors.New("no parameters error")
	te.mgr.paramProvider = faultyParamsProvider{err: paramsError}

	err := te.mgr.MaybeAdvanceWriteEpoch(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, paramsError)
}

func TestMaybeAdvanceEpoch_Error(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)
	te := newTestEnv(t)

	// Disable automatic epoch advancement and compaction to build up state
	te.mgr.compact = func(context.Context, []blob.ID, blob.ID) error {
		return nil
	}

	te.verifyCurrentWriteEpoch(t, 0)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	idxCount := p.GetEpochAdvanceOnCountThreshold()
	// Create sufficient indexes blobs and move clock forward to advance epoch.
	for i := range idxCount {
		te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(i))
	}

	// Advance the time so that the difference in times for writes will force
	// new epochs.
	te.ft.Advance(p.MinEpochDuration + 1*time.Hour)

	// one more to go over the threshold
	te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(idxCount))
	err = te.mgr.Refresh(ctx)

	require.NoError(t, err)
	te.verifyCurrentWriteEpoch(t, 0)

	berr := errors.New("advance epoch put blob error")
	te.faultyStorage.AddFaults(blobtesting.MethodPutBlob,
		fault.New().ErrorInstead(berr))

	err = te.mgr.MaybeAdvanceWriteEpoch(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, berr)
}

func TestForceAdvanceEpoch(t *testing.T) {
	te := newTestEnv(t)

	ctx := testlogging.Context(t)
	cs, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, cs.WriteEpoch)

	require.NoError(t, te.mgr.forceAdvanceEpoch(ctx))

	cs, err = te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, cs.WriteEpoch)

	require.NoError(t, te.mgr.forceAdvanceEpoch(ctx))

	cs, err = te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, cs.WriteEpoch)
}

func TestInvalid_WriteIndex(t *testing.T) {
	te := newTestEnv(t)

	ctx, cancel := context.WithCancel(testlogging.Context(t))
	defer cancel()

	// on first write, advance time enough to lose current context and go to the next epoch.
	te.faultyStorage.AddFault(blobtesting.MethodListBlobs).Repeat(100).Before(cancel).ErrorInstead(errors.New("canceled"))

	_, err := te.writeIndexFiles(ctx,
		newFakeIndexWithEntries(1),
		newFakeIndexWithEntries(2),
		newFakeIndexWithEntries(3),
	)

	require.ErrorIs(t, err, ctx.Err())
}

func TestInvalid_ForceAdvanceEpoch(t *testing.T) {
	te := newTestEnv(t)

	ctx, cancel := context.WithCancel(testlogging.Context(t))
	defer cancel()

	err := te.mgr.forceAdvanceEpoch(ctx)
	require.ErrorIs(t, err, ctx.Err())

	ctx = testlogging.Context(t)
	someError := errors.New("failed")
	te.faultyStorage.AddFault(blobtesting.MethodPutBlob).ErrorInstead(someError)

	err = te.mgr.forceAdvanceEpoch(ctx)
	require.ErrorIs(t, err, someError)
}

func TestInvalid_Cleanup(t *testing.T) {
	te := newTestEnv(t)

	ctx, cancel := context.WithCancel(testlogging.Context(t))
	cancel()

	err := te.mgr.CleanupSupersededIndexes(ctx)
	require.ErrorIs(t, err, ctx.Err())
}

//nolint:thelper
func verifySequentialWrites(t *testing.T, te *epochManagerTestEnv) {
	ctx := testlogging.Context(t)
	expected := &fakeIndex{}

	endTime := te.ft.NowFunc()().Add(90 * 24 * time.Hour)

	indexNum := 1
	lastDeletionWatermark := time.Time{}

	for te.ft.NowFunc()().Before(endTime) {
		indexNum++

		te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(indexNum))

		expected.Entries = append(expected.Entries, indexNum)
		te.verifyCompleteIndexSet(ctx, t, LatestEpoch, expected, lastDeletionWatermark)

		dt := randomTime(1*time.Minute, 8*time.Hour)
		t.Logf("advancing time by %v", dt)
		te.ft.Advance(dt)

		if indexNum%7 == 0 {
			require.NoError(t, te.mgr.Refresh(ctx))
		}

		if indexNum%27 == 0 {
			// do not require.NoError because we'll be sometimes inducing faults
			te.mgr.CleanupSupersededIndexes(ctx)
		}

		if indexNum%13 == 0 {
			ts := te.ft.NowFunc()().Truncate(time.Second)
			require.NoError(t, te.mgr.AdvanceDeletionWatermark(ctx, ts))
			require.NoError(t, te.mgr.AdvanceDeletionWatermark(ctx, ts.Add(-time.Second)))
			lastDeletionWatermark = ts
		}
	}

	te.mgr.Flush()

	for k, v := range te.data {
		t.Logf("data: %v (%v)", k, len(v))
	}

	t.Logf("total written %v", indexNum)
	t.Logf("total remaining %v", len(te.data))
}

func TestIndexEpochManager_Disabled(t *testing.T) {
	te := newTestEnv(t)

	te.mgr.paramProvider.(parameterProvider).Parameters.Enabled = false

	_, err := te.mgr.Current(testlogging.Context(t))
	require.Error(t, err)
}

func TestIndexEpochManager_RefreshContextCanceled(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	ctx, cancel := context.WithCancel(testlogging.Context(t))
	cancel()

	_, err := te.mgr.Current(ctx)
	require.ErrorIs(t, err, ctx.Err())
}

func TestMaybeCompactSingleEpoch_Empty(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	// this should be a no-op
	err := te.mgr.MaybeCompactSingleEpoch(ctx)

	require.NoError(t, err)
}

func TestMaybeCompactSingleEpoch_GetParametersError(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	paramsError := errors.New("no parameters error")
	te.mgr.paramProvider = faultyParamsProvider{err: paramsError}

	err := te.mgr.MaybeCompactSingleEpoch(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, paramsError)
}

func TestMaybeCompactSingleEpoch_CompactionError(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	idxCount := p.GetEpochAdvanceOnCountThreshold()
	// Create sufficient indexes blobs and move clock forward to advance epoch.
	for range 4 {
		for i := range idxCount {
			if i == idxCount-1 {
				// Advance the time so that the difference in times for writes will force
				// new epochs.
				te.ft.Advance(p.MinEpochDuration + 1*time.Hour)
			}

			te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(i))
		}

		require.NoError(t, te.mgr.MaybeAdvanceWriteEpoch(ctx))
	}

	compactionError := errors.New("test compaction error")
	te.mgr.compact = func(context.Context, []blob.ID, blob.ID) error {
		return compactionError
	}

	err = te.mgr.MaybeCompactSingleEpoch(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, compactionError)
}

func TestMaybeCompactSingleEpoch(t *testing.T) {
	const epochsToWrite = 5

	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	idxCount := p.GetEpochAdvanceOnCountThreshold()

	var k int

	// Create sufficient indexes blobs and move clock forward to advance current epoch
	for j := range epochsToWrite {
		for i := range idxCount {
			if i == idxCount-1 {
				// Advance the time so that the difference in times for writes will force
				// new epochs.
				te.ft.Advance(p.MinEpochDuration + 1*time.Hour)
			}

			te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(k))
			k++
		}

		te.verifyCurrentWriteEpoch(t, j)

		err = te.mgr.MaybeAdvanceWriteEpoch(ctx)
		require.NoError(t, err)

		err = te.mgr.Refresh(ctx) // force state refresh

		require.NoError(t, err)
		te.verifyCurrentWriteEpoch(t, j+1)
	}

	cs, err := te.mgr.Current(ctx)

	require.NoError(t, err)
	require.Equal(t, epochsToWrite, cs.WriteEpoch)

	// no epochs have been compacted, so the compacted set should be empty and
	// the uncompacted epoch set should have all the epochs
	require.Empty(t, cs.LongestRangeCheckpointSets)
	require.Empty(t, cs.SingleEpochCompactionSets)

	// perform single-epoch compaction for settled epochs
	newestEpochToCompact := cs.WriteEpoch - numUnsettledEpochs + 1
	for j := range newestEpochToCompact {
		err = te.mgr.MaybeCompactSingleEpoch(ctx)
		require.NoError(t, err)

		err = te.mgr.Refresh(ctx) // force state refresh
		require.NoError(t, err)

		cs, err = te.mgr.Current(ctx)
		require.NoError(t, err)

		require.Len(t, cs.SingleEpochCompactionSets, j+1)
	}

	require.Len(t, cs.SingleEpochCompactionSets, newestEpochToCompact)

	// no more epochs should be compacted at this point
	err = te.mgr.MaybeCompactSingleEpoch(ctx)
	require.NoError(t, err)

	err = te.mgr.Refresh(ctx)
	require.NoError(t, err)

	cs, err = te.mgr.Current(ctx)
	require.NoError(t, err)

	require.Len(t, cs.SingleEpochCompactionSets, newestEpochToCompact)
}

func TestMaybeGenerateRangeCheckpoint_Empty(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	// this should be a no-op
	err := te.mgr.MaybeGenerateRangeCheckpoint(ctx)

	require.NoError(t, err)
}

func TestMaybeGenerateRangeCheckpoint_GetParametersError(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	paramsError := errors.New("no parameters error")
	te.mgr.paramProvider = faultyParamsProvider{err: paramsError}

	err := te.mgr.MaybeGenerateRangeCheckpoint(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, paramsError)
}

func TestMaybeGenerateRangeCheckpoint_FailToReadState(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	ctx, cancel := context.WithCancel(ctx)

	cancel()

	err := te.mgr.MaybeGenerateRangeCheckpoint(ctx)

	require.Error(t, err)
}

func TestMaybeGenerateRangeCheckpoint_CompactionError(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	epochsToWrite := p.FullCheckpointFrequency + 3
	idxCount := p.GetEpochAdvanceOnCountThreshold()

	var k int

	// Create sufficient indexes blobs and move clock forward to advance epoch.
	for range epochsToWrite {
		for i := range idxCount {
			if i == idxCount-1 {
				// Advance the time so that the difference in times for writes will force
				// new epochs.
				te.ft.Advance(p.MinEpochDuration + 1*time.Hour)
			}

			te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(k))
			k++
		}

		err = te.mgr.MaybeAdvanceWriteEpoch(ctx)
		require.NoError(t, err)

		err = te.mgr.Refresh(ctx)
		require.NoError(t, err)
	}

	cs, err := te.mgr.Current(ctx)

	require.NoError(t, err)
	require.Equal(t, epochsToWrite, cs.WriteEpoch)

	compactionError := errors.New("test compaction error")
	te.mgr.compact = func(context.Context, []blob.ID, blob.ID) error {
		return compactionError
	}

	err = te.mgr.MaybeGenerateRangeCheckpoint(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, compactionError)
}

func TestMaybeGenerateRangeCheckpoint_FromUncompactedEpochs(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	var k int

	epochsToWrite := p.FullCheckpointFrequency + 3
	idxCount := p.GetEpochAdvanceOnCountThreshold()
	// Create sufficient indexes blobs and move clock forward to advance epoch.
	for range epochsToWrite {
		for i := range idxCount {
			if i == idxCount-1 {
				// Advance the time so that the difference in times for writes will force
				// new epochs.
				te.ft.Advance(p.MinEpochDuration + 1*time.Hour)
			}

			te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(k))
		}

		err = te.mgr.MaybeAdvanceWriteEpoch(ctx)
		require.NoError(t, err)

		err = te.mgr.Refresh(ctx)
		require.NoError(t, err)
	}

	cs, err := te.mgr.Current(ctx)

	require.NoError(t, err)
	require.Equal(t, epochsToWrite, cs.WriteEpoch)
	require.Empty(t, cs.LongestRangeCheckpointSets)

	err = te.mgr.MaybeGenerateRangeCheckpoint(ctx)
	require.NoError(t, err)

	err = te.mgr.Refresh(ctx)
	require.NoError(t, err)

	cs, err = te.mgr.Current(ctx)

	require.NoError(t, err)
	require.Equal(t, epochsToWrite, cs.WriteEpoch)
	require.Len(t, cs.LongestRangeCheckpointSets, 1)
}

func TestMaybeGenerateRangeCheckpoint_FromCompactedEpochs(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	var k int

	epochsToWrite := p.FullCheckpointFrequency + 3
	idxCount := p.GetEpochAdvanceOnCountThreshold()
	// Create sufficient indexes blobs and move clock forward to advance epoch.
	for range epochsToWrite {
		for i := range idxCount {
			if i == idxCount-1 {
				// Advance the time so that the difference in times for writes will force
				// new epochs.
				te.ft.Advance(p.MinEpochDuration + 1*time.Hour)
			}

			te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(k))
		}

		err = te.mgr.MaybeAdvanceWriteEpoch(ctx)
		require.NoError(t, err)

		err = te.mgr.Refresh(ctx)
		require.NoError(t, err)
	}

	cs, err := te.mgr.Current(ctx)
	require.NoError(t, err)

	require.Equal(t, epochsToWrite, cs.WriteEpoch)

	// perform single-epoch compaction for settled epochs
	newestEpochToCompact := cs.WriteEpoch - numUnsettledEpochs + 1
	for j := range newestEpochToCompact {
		err = te.mgr.MaybeCompactSingleEpoch(ctx)
		require.NoError(t, err)

		err = te.mgr.Refresh(ctx) // force state refresh
		require.NoError(t, err)

		cs, err = te.mgr.Current(ctx)
		require.NoError(t, err)

		require.Len(t, cs.SingleEpochCompactionSets, j+1)
	}

	cs, err = te.mgr.Current(ctx)

	require.NoError(t, err)
	require.Equal(t, epochsToWrite, cs.WriteEpoch)
	require.Empty(t, cs.LongestRangeCheckpointSets)

	err = te.mgr.MaybeGenerateRangeCheckpoint(ctx)
	require.NoError(t, err)

	err = te.mgr.Refresh(ctx)
	require.NoError(t, err)

	cs, err = te.mgr.Current(ctx)

	require.NoError(t, err)
	require.Equal(t, epochsToWrite, cs.WriteEpoch)
	require.Len(t, cs.LongestRangeCheckpointSets, 1)
}

func TestValidateParameters(t *testing.T) {
	cases := []struct {
		p       Parameters
		wantErr string
	}{
		{DefaultParameters(), ""},
		{
			Parameters{
				Enabled: false,
			}, "",
		},
		{
			Parameters{
				Enabled:          true,
				MinEpochDuration: 1 * time.Second,
			}, "minimum epoch duration too low: 1s",
		},
		{
			Parameters{
				Enabled:               true,
				MinEpochDuration:      1 * time.Hour,
				EpochRefreshFrequency: 30 * time.Minute,
			}, "epoch refresh period is too long, must be 1/3 of minimal epoch duration or shorter",
		},
		{
			Parameters{
				Enabled:                 true,
				MinEpochDuration:        1 * time.Hour,
				EpochRefreshFrequency:   10 * time.Minute,
				FullCheckpointFrequency: -1,
			}, "invalid epoch checkpoint frequency",
		},
		{
			Parameters{
				Enabled:                 true,
				MinEpochDuration:        1 * time.Hour,
				EpochRefreshFrequency:   10 * time.Minute,
				FullCheckpointFrequency: 5,
				CleanupSafetyMargin:     15 * time.Minute,
			}, "invalid cleanup safety margin, must be at least 3x epoch refresh frequency",
		},
		{
			Parameters{
				Enabled:                      true,
				MinEpochDuration:             1 * time.Hour,
				EpochRefreshFrequency:        10 * time.Minute,
				FullCheckpointFrequency:      5,
				CleanupSafetyMargin:          time.Hour,
				EpochAdvanceOnCountThreshold: 1,
			}, "epoch advance on count too low",
		},
		{
			Parameters{
				Enabled:                      true,
				MinEpochDuration:             1 * time.Hour,
				EpochRefreshFrequency:        10 * time.Minute,
				FullCheckpointFrequency:      5,
				CleanupSafetyMargin:          time.Hour,
				EpochAdvanceOnCountThreshold: 10,
			}, "epoch advance on size too low",
		},
	}

	for _, tc := range cases {
		err := tc.p.Validate()
		if tc.wantErr != "" {
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestCleanupMarkers_Empty(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	// this should be a no-op
	err := te.mgr.CleanupMarkers(ctx)

	require.NoError(t, err)
}

func TestCleanupMarkers_GetParametersError(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	paramsError := errors.New("no parameters error")
	te.mgr.paramProvider = faultyParamsProvider{err: paramsError}

	err := te.mgr.CleanupMarkers(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, paramsError)
}

func TestCleanupMarkers_FailToReadState(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx, cancel := context.WithCancel(testlogging.Context(t))

	te.ft.Advance(1 * time.Hour) // force state refresh in CleanupMarkers

	cancel()
	err := te.mgr.CleanupMarkers(ctx)

	require.Error(t, err)
}

func TestCleanupMarkers_AvoidCleaningUpSingleEpochMarker(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	te.mgr.forceAdvanceEpoch(ctx)
	te.ft.Advance(1 * time.Hour)

	require.NoError(t, te.mgr.Refresh(ctx))

	cs, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, cs.WriteEpoch)

	err = te.mgr.CleanupMarkers(ctx)
	require.NoError(t, err)

	require.NoError(t, te.mgr.Refresh(ctx))

	// is the epoch marker preserved?
	te.verifyCurrentWriteEpoch(t, 1)

	cs, err = te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Len(t, cs.EpochMarkerBlobs, 1)
}

func TestCleanupMarkers_CleanUpManyMarkers(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)
	ctx := testlogging.Context(t)

	p, err := te.mgr.getParameters(ctx)
	require.NoError(t, err)

	const epochsToAdvance = 5

	te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(0))
	for i := range epochsToAdvance {
		te.ft.Advance(p.MinEpochDuration + 1*time.Hour)
		te.mgr.forceAdvanceEpoch(ctx)
		te.mustWriteIndexFiles(ctx, t, newFakeIndexWithEntries(i+1))
	}

	require.NoError(t, te.mgr.Refresh(ctx))
	te.verifyCurrentWriteEpoch(t, epochsToAdvance)

	cs, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Len(t, cs.EpochMarkerBlobs, epochsToAdvance)

	err = te.mgr.CleanupMarkers(ctx)
	require.NoError(t, err)

	// is the epoch marker preserved?
	require.NoError(t, te.mgr.Refresh(ctx))
	te.verifyCurrentWriteEpoch(t, epochsToAdvance)

	cs, err = te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Len(t, cs.EpochMarkerBlobs, 2) // at least 2 epoch markers are kept
}

func randomTime(minTime, maxTime time.Duration) time.Duration {
	return time.Duration(float64(maxTime-minTime)*rand.Float64() + float64(minTime))
}

func (te *epochManagerTestEnv) verifyCompleteIndexSet(ctx context.Context, t *testing.T, maxEpoch int, want *fakeIndex, wantDeletionWatermark time.Time) {
	t.Helper()

	blobs, deletionWatermark, err := te.mgr.GetCompleteIndexSet(ctx, maxEpoch)
	t.Logf("complete set length: %v", len(blobs))
	require.NoError(t, err)

	merged, err := te.getMergedIndexContents(ctx, blob.IDsFromMetadata(blobs))
	require.NoError(t, err)
	require.Equal(t, want.Entries, merged.Entries)
	require.True(t, wantDeletionWatermark.Equal(deletionWatermark), "invalid deletion watermark %v %v", deletionWatermark, wantDeletionWatermark)
}

func (te *epochManagerTestEnv) getMergedIndexContents(ctx context.Context, blobIDs []blob.ID) (*fakeIndex, error) {
	result := &fakeIndex{}

	var v gather.WriteBuffer
	defer v.Close()

	for _, blobID := range blobIDs {
		err := te.unloggedst.GetBlob(ctx, blobID, 0, -1, &v)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get blob")
		}

		ndx, err := parseFakeIndex(v.ToByteSlice())
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse fake index")
		}

		result.Entries = append(result.Entries, ndx.Entries...)
	}

	sort.Ints(result.Entries)

	return result, nil
}

func (te *epochManagerTestEnv) writeIndexFiles(ctx context.Context, ndx ...*fakeIndex) ([]blob.Metadata, error) {
	shards := map[blob.ID]blob.Bytes{}
	sessionID := rand.Uint64()

	for _, n := range ndx {
		rnd := rand.Uint64()

		shards[blob.ID(fmt.Sprintf("%0x-c%v-s%0x", rnd, len(ndx), sessionID))] = gather.FromSlice(n.Bytes())
	}

	return te.mgr.WriteIndex(ctx, shards)
}

func (te *epochManagerTestEnv) mustWriteIndexFiles(ctx context.Context, t *testing.T, ndx ...*fakeIndex) {
	t.Helper()

	_, err := te.writeIndexFiles(ctx, ndx...)

	require.NoError(t, err)
}

type parameterProvider struct {
	*Parameters
}

func (p parameterProvider) GetParameters(ctx context.Context) (*Parameters, error) {
	return p.Parameters, nil
}

// forceAdvanceEpoch advances current epoch unconditionally.
func (e *Manager) forceAdvanceEpoch(ctx context.Context) error {
	cs, err := e.committedState(ctx, 0)
	if err != nil {
		return err
	}

	e.Invalidate()

	if err := e.advanceEpochMarker(ctx, cs); err != nil {
		return errors.Wrap(err, "error advancing epoch")
	}

	return nil
}

func (te *epochManagerTestEnv) verifyCurrentWriteEpoch(t *testing.T, expectedEpoch int) {
	t.Helper()

	// load current epoch directly from index blobs in the backend storage
	cs := CurrentSnapshot{
		WriteEpoch:                0,
		EpochStartTime:            map[int]time.Time{},
		UncompactedEpochSets:      map[int][]blob.Metadata{},
		SingleEpochCompactionSets: map[int][]blob.Metadata{},
	}

	ctx := testlogging.Context(t)
	err := te.mgr.loadWriteEpoch(ctx, &cs)

	require.NoError(t, err)
	require.Equal(t, expectedEpoch, cs.WriteEpoch)

	// check current epoch via the epoch manager, this may or may not cause
	// a refresh from storage.
	cs, err = te.mgr.Current(ctx)

	require.NoError(t, err)
	require.Equal(t, expectedEpoch, cs.WriteEpoch)
}
