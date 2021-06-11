package epoch

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
)

const verifyAllEpochs = -1

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
		te.st.PutBlob(ctx, blob.ID(fmt.Sprintf("%v%016x-s0-c1", prefix, rand.Int63())), gather.FromSlice(merged.Bytes())),
		"PutBlob error")
}

//  write two dummy compaction blobs instead of 3, simulating a compaction that crashed before fully complete.
func (te *epochManagerTestEnv) interruptedCompaction(ctx context.Context, _ []blob.ID, prefix blob.ID) error {
	sess := rand.Int63()

	te.st.PutBlob(ctx, blob.ID(fmt.Sprintf("%v%016x-s%v-c3", prefix, sess, rand.Int63())), gather.FromSlice([]byte("dummy")))
	te.st.PutBlob(ctx, blob.ID(fmt.Sprintf("%v%016x-s%v-c3", prefix, sess, rand.Int63())), gather.FromSlice([]byte("dummy")))

	return errors.Errorf("failed for some reason")
}

func newTestEnv(t *testing.T) *epochManagerTestEnv {
	t.Helper()

	data := blobtesting.DataMap{}
	ft := faketime.NewClockTimeWithOffset(0)
	st := blobtesting.NewMapStorage(data, nil, ft.NowFunc())
	unloggedst := st
	fs := &blobtesting.FaultyStorage{
		Base: st,
	}
	st = fs
	st = logging.NewWrapper(st, t.Logf, "[STORAGE] ")
	te := &epochManagerTestEnv{unloggedst: unloggedst, st: st, ft: ft}
	m := NewManager(te.st, Parameters{
		EpochRefreshFrequency:                 20 * time.Minute,
		FullCheckpointFrequency:               7,
		CleanupSafetyMargin:                   1 * time.Hour,
		MinEpochDuration:                      12 * time.Hour,
		EpochAdvanceOnCountThreshold:          25,
		EpochAdvanceOnTotalSizeBytesThreshold: 20 << 20,
		DeleteParallelism:                     1,
	}, te.compact, testlogging.NewTestLogger(t))
	m.timeFunc = te.ft.NowFunc()
	te.mgr = m
	te.faultyStorage = fs
	te.data = data

	return te
}

func TestIndexEpochManager_Regular(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	verifySequentialWrites(t, te)
}

func TestIndexEpochManager_RogueBlobs(t *testing.T) {
	t.Parallel()

	te := newTestEnv(t)

	te.data[epochMarkerIndexBlobPrefix+"zzzz"] = []byte{1}
	te.data[singleEpochCompactionBlobPrefix+"zzzz"] = []byte{1}
	te.data[fullCheckpointIndexBlobPrefix+"zzzz"] = []byte{1}

	verifySequentialWrites(t, te)
	te.mgr.Cleanup(testlogging.Context(t))
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
		return nil
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
	te.faultyStorage.Faults = map[string][]*blobtesting.Fault{
		"DeleteBlob": {
			{Repeat: math.MaxInt32, Err: errors.Errorf("something bad happened")},
		},
	}

	// set up test environment in which compactions never succeed for whatever reason.
	te.mgr.compact = func(ctx context.Context, blobIDs []blob.ID, outputPrefix blob.ID) error {
		if rand.Intn(100) < 20 {
			return te.interruptedCompaction(ctx, blobIDs, outputPrefix)
		}

		return te.compact(ctx, blobIDs, outputPrefix)
	}

	verifySequentialWrites(t, te)
}

func TestRefreshRetriesIfTakingTooLong(t *testing.T) {
	te := newTestEnv(t)
	defer te.mgr.Flush()

	cnt := 0

	te.faultyStorage.Faults = map[string][]*blobtesting.Fault{
		"ListBlobs": {
			&blobtesting.Fault{
				Repeat: 4, // refresh does 3 lists, so this will cause 2 unsuccessful retries
				ErrCallback: func() error {
					cnt++
					te.ft.Advance(24 * time.Hour)

					return nil
				},
			},
		},
	}

	ctx := testlogging.Context(t)

	require.NoError(t, te.mgr.Refresh(ctx))

	require.EqualValues(t, 2, *te.mgr.committedStateRefreshTooSlow)
}

func TestGetCompleteIndexSetRetriesIfTookTooLong(t *testing.T) {
	te := newTestEnv(t)
	defer te.mgr.Flush()

	ctx := testlogging.Context(t)

	// load committed state
	require.NoError(t, te.mgr.Refresh(ctx))

	cnt := 0

	te.faultyStorage.Faults = map[string][]*blobtesting.Fault{
		"ListBlobs": {
			&blobtesting.Fault{
				Repeat: 1000,
				ErrCallback: func() error {
					cnt++
					if cnt == 1 {
						te.ft.Advance(24 * time.Hour)
					}
					return nil
				},
			},
		},
	}

	_, err := te.mgr.GetCompleteIndexSet(ctx, 0)
	require.NoError(t, err)

	require.EqualValues(t, 1, *te.mgr.getCompleteIndexSetTooSlow)
}

func TestLateWriteIsIgnored(t *testing.T) {
	te := newTestEnv(t)
	defer te.mgr.Flush()

	ctx := testlogging.Context(t)

	// get current epoch number
	epoch, err := te.mgr.Current(ctx)
	require.NoError(t, err)

	var rnd [8]byte

	rand.Read(rnd[:])

	blobID1 := blob.ID(fmt.Sprintf("%v%v_%x", uncompactedIndexBlobPrefix, epoch, rnd[:]))

	rand.Read(rnd[:])
	blobID2 := blob.ID(fmt.Sprintf("%v%v_%x", uncompactedIndexBlobPrefix, epoch, rnd[:]))

	// at this point it's possible that the process hangs for a very long time, during which the
	// current epoch moves by 2. This would be dangerous, since we'd potentially modify an already
	// settled epoch.
	// To verify this, we call WroteIndex() after the write which will fail if the write finished
	// late. During read we will ignore index files with dates that are too late.

	// simulate process process hanging for a very long time, during which time the epoch moves.
	for i := 0; i < 30; i++ {
		te.mustWriteIndexFile(ctx, t, newFakeIndexWithEntries(100+i))
		te.ft.Advance(time.Hour)
	}

	// epoch advance is triggered during reads.
	_, err = te.mgr.GetCompleteIndexSet(ctx, epoch+1)
	require.NoError(t, err)

	// make sure the epoch has moved
	epoch2, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, epoch+1, epoch2)

	require.NoError(t, te.st.PutBlob(ctx, blobID1, gather.FromSlice([]byte("dummy"))))
	bm, err := te.unloggedst.GetMetadata(ctx, blobID1)

	require.NoError(t, err)

	// it's not an error to finish the write in the next epoch.
	require.NoError(t, te.mgr.WroteIndex(ctx, bm))

	// move the epoch one more.
	for i := 0; i < 30; i++ {
		te.mustWriteIndexFile(ctx, t, newFakeIndexWithEntries(100+i))
		te.ft.Advance(time.Hour)
	}

	// epoch advance is triggered during reads.
	_, err = te.mgr.GetCompleteIndexSet(ctx, epoch+2)
	require.NoError(t, err)

	// make sure the epoch has moved
	epoch3, err := te.mgr.Current(ctx)
	require.NoError(t, err)
	require.Equal(t, epoch+2, epoch3)

	require.NoError(t, te.st.PutBlob(ctx, blobID2, gather.FromSlice([]byte("dummy"))))
	bm, err = te.unloggedst.GetMetadata(ctx, blobID2)

	require.NoError(t, err)

	// at this point WroteIndex() will fail because epoch #0 is already settled.
	require.Error(t, te.mgr.WroteIndex(ctx, bm))

	iset, err := te.mgr.GetCompleteIndexSet(ctx, epoch3)
	require.NoError(t, err)

	// blobID1 will be included in the index.
	require.Contains(t, iset, blobID1)

	// blobID2 will be excluded from the index.
	require.NotContains(t, iset, blobID2)
}

// nolint:thelper
func verifySequentialWrites(t *testing.T, te *epochManagerTestEnv) {
	ctx := testlogging.Context(t)
	expected := &fakeIndex{}

	endTime := te.ft.NowFunc()().Add(90 * 24 * time.Hour)

	indexNum := 1

	for te.ft.NowFunc()().Before(endTime) {
		indexNum++

		te.mustWriteIndexFile(ctx, t, newFakeIndexWithEntries(indexNum))

		expected.Entries = append(expected.Entries, indexNum)
		te.verifyCompleteIndexSet(ctx, t, verifyAllEpochs, expected)

		dt := randomTime(1*time.Minute, 8*time.Hour)
		t.Logf("advancing time by %v", dt)
		te.ft.Advance(dt)

		if indexNum%7 == 0 {
			require.NoError(t, te.mgr.Refresh(ctx))
		}

		if indexNum%27 == 0 {
			// do not require.NoError because we'll be sometimes inducing faults
			te.mgr.Cleanup(ctx)
		}
	}

	te.mgr.Flush()

	for k, v := range te.data {
		t.Logf("data: %v (%v)", k, len(v))
	}

	t.Logf("total written %v", indexNum)
	t.Logf("total remaining %v", len(te.data))
}

func randomTime(min, max time.Duration) time.Duration {
	return time.Duration(float64(max-min)*rand.Float64() + float64(min))
}

func (te *epochManagerTestEnv) verifyCompleteIndexSet(ctx context.Context, t *testing.T, maxEpoch int, want *fakeIndex) {
	t.Helper()

	if maxEpoch == verifyAllEpochs {
		n, err := te.mgr.Current(ctx)
		require.NoError(t, err)

		maxEpoch = n + 1
	}

	blobs, err := te.mgr.GetCompleteIndexSet(ctx, maxEpoch)
	t.Logf("complete set length: %v", len(blobs))
	require.NoError(t, err)

	merged, err := te.getMergedIndexContents(ctx, blobs)
	require.NoError(t, err)
	require.Equal(t, want.Entries, merged.Entries)
}

func (te *epochManagerTestEnv) getMergedIndexContents(ctx context.Context, blobIDs []blob.ID) (*fakeIndex, error) {
	result := &fakeIndex{}

	for _, blobID := range blobIDs {
		v, err := te.unloggedst.GetBlob(ctx, blobID, 0, -1)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get blob")
		}

		ndx, err := parseFakeIndex(v)
		if err != nil {
			return nil, errors.Wrap(err, "unable to parse fake index")
		}

		result.Entries = append(result.Entries, ndx.Entries...)
	}

	sort.Ints(result.Entries)

	return result, nil
}

func (te *epochManagerTestEnv) mustWriteIndexFile(ctx context.Context, t *testing.T, ndx *fakeIndex) {
	t.Helper()

	epoch, err := te.mgr.Current(ctx)
	require.NoError(t, err)

	var rnd [8]byte

	rand.Read(rnd[:])

	blobID := blob.ID(fmt.Sprintf("%v%v_%x", uncompactedIndexBlobPrefix, epoch, rnd[:]))

	require.NoError(t, te.st.PutBlob(ctx, blobID, gather.FromSlice(ndx.Bytes())))
	bm, err := te.unloggedst.GetMetadata(ctx, blobID)

	require.NoError(t, err)
	require.NoError(t, te.mgr.WroteIndex(ctx, bm))
}
