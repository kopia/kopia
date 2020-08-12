package content

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/hashing"
)

// we use two fake time sources - one for local client and one for the remote store
// to simulate clock drift.
var (
	fakeLocalStartTime = time.Date(2020, 1, 1, 14, 0, 0, 0, time.UTC)
	fakeStoreStartTime = time.Date(2020, 1, 1, 10, 0, 0, 0, time.UTC)
)

const (
	testIndexBlobDeleteAge            = 1 * time.Minute
	testEventualConsistencySettleTime = 45 * time.Second
)

func TestIndexBlobManager(t *testing.T) {
	cases := []struct {
		storageTimeAdvanceBetweenCompactions time.Duration
		wantIndexCount                       int
		wantCompactionLogCount               int
		wantCleanupCount                     int
	}{
		{
			// we write 6 index blobs and 2 compaction logs
			// but not enough time has passed to delete anything
			storageTimeAdvanceBetweenCompactions: 0,
			wantIndexCount:                       6,
			wantCompactionLogCount:               2,
		},
		{
			// we write 6 index blobs and 2 compaction logs
			// enough time has passed to delete 3 indexes and create cleanup log
			storageTimeAdvanceBetweenCompactions: testIndexBlobDeleteAge + 1*time.Second,
			wantIndexCount:                       3,
			wantCompactionLogCount:               2,
			wantCleanupCount:                     1,
		},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			// fake underlying blob store with fake time
			storageData := blobtesting.DataMap{}

			fakeLocalTime := faketime.NewTimeAdvance(fakeLocalStartTime)
			fakeStorageTime := faketime.NewTimeAdvance(fakeStoreStartTime)

			st := blobtesting.NewMapStorage(storageData, nil, fakeStorageTime.NowFunc())
			st = blobtesting.NewEventuallyConsistentStorage(st, testEventualConsistencySettleTime, fakeStorageTime.NowFunc())
			m := newIndexBlobManagerForTesting(t, st, fakeLocalTime.NowFunc())

			assertIndexBlobList(t, m)

			b1 := mustWriteIndexBlob(t, m, "index-1")
			assertIndexBlobList(t, m, b1)
			fakeStorageTime.Advance(1 * time.Second)

			b2 := mustWriteIndexBlob(t, m, "index-2")
			assertIndexBlobList(t, m, b1, b2)
			fakeStorageTime.Advance(1 * time.Second)

			b3 := mustWriteIndexBlob(t, m, "index-3")
			assertIndexBlobList(t, m, b1, b2, b3)
			fakeStorageTime.Advance(1 * time.Second)

			b4 := mustWriteIndexBlob(t, m, "index-4")
			assertIndexBlobList(t, m, b1, b2, b3, b4)
			fakeStorageTime.Advance(1 * time.Second)
			assertBlobCounts(t, storageData, 4, 0, 0)

			// first compaction b1+b2+b3=>b4
			mustRegisterCompaction(t, m, []blob.Metadata{b1, b2, b3}, []blob.Metadata{b4})

			assertIndexBlobList(t, m, b4)
			fakeStorageTime.Advance(tc.storageTimeAdvanceBetweenCompactions)

			// second compaction b4+b5=>b6
			b5 := mustWriteIndexBlob(t, m, "index-5")
			b6 := mustWriteIndexBlob(t, m, "index-6")
			mustRegisterCompaction(t, m, []blob.Metadata{b4, b5}, []blob.Metadata{b6})
			assertIndexBlobList(t, m, b6)
			assertBlobCounts(t, storageData, tc.wantIndexCount, tc.wantCompactionLogCount, tc.wantCleanupCount)
		})
	}
}

type action int

const (
	actionWrite                 = 1
	actionRead                  = 2
	actionCompact               = 3
	actionDelete                = 4
	actionUndelete              = 5
	actionCompactAndDropDeleted = 6
)

// actionsTestIndexBlobManagerStress is a set of actionsTestIndexBlobManagerStress by each actor performed in TestIndexBlobManagerStress with weights.
var actionsTestIndexBlobManagerStress = []struct {
	a      action
	weight int
}{
	{actionWrite, 10},
	{actionRead, 10},
	{actionCompact, 10},
	{actionDelete, 10},
	{actionUndelete, 10},
	{actionCompactAndDropDeleted, 10},
}

func pickRandomActionTestIndexBlobManagerStress() action {
	sum := 0
	for _, a := range actionsTestIndexBlobManagerStress {
		sum += a.weight
	}

	n := rand.Intn(sum)
	for _, a := range actionsTestIndexBlobManagerStress {
		if n < a.weight {
			return a.a
		}

		n -= a.weight
	}

	panic("impossible")
}

// TestIndexBlobManagerStress launches N actors, each randomly writing new index blobs,
// verifying that all blobs previously written by it are correct and randomly compacting blobs.
// nolint:gocyclo
func TestIndexBlobManagerStress(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().UnixNano())

	for i := range actionsTestIndexBlobManagerStress {
		actionsTestIndexBlobManagerStress[i].weight = rand.Intn(100)
		t.Logf("weight[%v] = %v", i, actionsTestIndexBlobManagerStress[i].weight)
	}

	var (
		fakeTimeFunc      = faketime.AutoAdvance(fakeLocalStartTime, 100*time.Millisecond)
		deadline          time.Time // when (according to fakeTimeFunc should the test finish)
		localTimeDeadline time.Time // when (according to time.Now, the test should finish)
	)

	localTimeDeadline = time.Now().Add(30 * time.Second)

	if os.Getenv("CI") != "" {
		// when running on CI, simulate 4 hours, this takes about ~15-20 seconds.
		deadline = fakeTimeFunc().Add(4 * time.Hour)
	} else {
		// otherwise test only 1 hour, which still provides decent coverage, takes about 3-5 seconds.
		deadline = fakeTimeFunc().Add(1 * time.Hour)
	}

	// shared storage
	st := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, fakeTimeFunc)

	var eg errgroup.Group

	numActors := 2

	for actorID := 0; actorID < numActors; actorID++ {
		actorID := actorID
		loggedSt := logging.NewWrapper(st, func(m string, args ...interface{}) {
			t.Logf(fmt.Sprintf("@%v actor[%v]:", fakeTimeFunc().Format("150405.000"), actorID)+m, args...)
		}, "")
		contentPrefix := fmt.Sprintf("a%v", actorID)

		eg.Go(func() error {
			numWritten := 0
			deletedContents := map[string]bool{}
			ctx := testlogging.ContextWithLevelAndPrefixFunc(t, testlogging.LevelDebug, func() string {
				return fmt.Sprintf("@%v actor[%v]:", fakeTimeFunc().Format("150405.000"), actorID)
			})

			m := newIndexBlobManagerForTesting(t, loggedSt, fakeTimeFunc)

			// run stress test until the deadline, aborting early on any failure
			for fakeTimeFunc().Before(deadline) && time.Now().Before(localTimeDeadline) {
				switch pickRandomActionTestIndexBlobManagerStress() {
				case actionRead:
					if err := verifyFakeContentsWritten(ctx, m, numWritten, contentPrefix, deletedContents); err != nil {
						return errors.Wrapf(err, "actor[%v] error verifying contents", actorID)
					}

				case actionWrite:
					if err := writeFakeContents(ctx, m, contentPrefix, rand.Intn(10)+5, &numWritten, fakeTimeFunc); err != nil {
						return errors.Wrapf(err, "actor[%v] write error", actorID)
					}

				case actionDelete:
					if err := deleteFakeContents(ctx, m, contentPrefix, numWritten, deletedContents, fakeTimeFunc); err != nil {
						return errors.Wrapf(err, "actor[%v] delete error", actorID)
					}

				case actionUndelete:
					if err := undeleteFakeContents(ctx, m, deletedContents, fakeTimeFunc); err != nil {
						return errors.Wrapf(err, "actor[%v] undelete error", actorID)
					}

				case actionCompact:
					// compaction by more than one actor is unsafe, do it only if actorID == 0
					if actorID != 0 {
						continue
					}

					if err := fakeCompaction(ctx, m, false); err != nil {
						return errors.Wrapf(err, "actor[%v] compaction error", actorID)
					}

				case actionCompactAndDropDeleted:
					// compaction by more than one actor is unsafe, do it only if actorID == 0
					if actorID != 0 {
						continue
					}

					if err := fakeCompaction(ctx, m, true); err != nil {
						return errors.Wrapf(err, "actor[%v] compaction error", actorID)
					}
				}
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Errorf("err: %+v", err)
	}
}

func TestIndexBlobManagerPreventsResurrectOfDeletedContents(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// the test is randomized and runs very quickly, run it lots of times
	failed := false
	for i := 0; i < 100 && !failed; i++ {
		t.Run(fmt.Sprintf("attempt-%v", i), func(t *testing.T) {
			verifyIndexBlobManagerPreventsResurrectOfDeletedContents(
				t, 1*time.Second, 1*time.Second, testIndexBlobDeleteAge, 1*time.Second, 2*time.Second,
			)
		})
	}
}

func TestCompactionCreatesPreviousIndex(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	storageData := blobtesting.DataMap{}

	fakeTime := faketime.NewTimeAdvance(fakeLocalStartTime)
	fakeTimeFunc := fakeTime.NowFunc()

	st := blobtesting.NewMapStorage(storageData, nil, fakeTimeFunc)
	st = blobtesting.NewEventuallyConsistentStorage(st, testEventualConsistencySettleTime, fakeTimeFunc)
	st = logging.NewWrapper(st, func(msg string, args ...interface{}) {
		t.Logf("[store] "+fakeTimeFunc().Format("150405.000")+" "+msg, args...)
	}, "store: ")
	m := newIndexBlobManagerForTesting(t, st, fakeTimeFunc)

	numWritten := 0
	deleted := map[string]bool{}

	prefix := "prefix"
	ctx := testlogging.ContextWithLevelAndPrefixFunc(t, testlogging.LevelDebug, func() string {
		return fakeTimeFunc().Format("150405.000") + " "
	})

	// index#1 - add content1
	must(t, writeFakeContents(ctx, m, prefix, 1, &numWritten, fakeTimeFunc))
	fakeTime.Advance(1 * time.Second)

	// index#2 - add content2
	must(t, writeFakeContents(ctx, m, prefix, 1, &numWritten, fakeTimeFunc))
	fakeTime.Advance(1 * time.Second)

	// index#3 - {content1, content2}, index#1, index#2 marked for deletion
	must(t, fakeCompaction(ctx, m, false))
	fakeTime.Advance(1 * time.Second)

	// index#4 - delete content1
	must(t, deleteFakeContents(ctx, m, prefix, 1, deleted, fakeTimeFunc))
	fakeTime.Advance(1 * time.Second)

	// this will create index identical to index#2,
	// we will embed random ID in the index to ensure that they get different blob ID each time.
	// otherwise (since indexes are based on hash of content) they would create the same blob ID.
	// if this was the case, first compaction marks index#1 as deleted and second compaction
	// revives it.
	must(t, fakeCompaction(ctx, m, true))
	fakeTime.Advance(testEventualConsistencySettleTime)

	// if we were not to add randomness to index blobs, this would fail.
	must(t, verifyFakeContentsWritten(ctx, m, 2, prefix, deleted))
}

func TestIndexBlobManagerPreventsResurrectOfDeletedContents_RandomizedTimings(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// the test is randomized and runs very quickly, run it lots of times
	for i := 0; i < 1000; i++ {
		t.Run(fmt.Sprintf("attempt-%v", i), func(t *testing.T) {
			verifyIndexBlobManagerPreventsResurrectOfDeletedContents(
				t,
				randomDuration(10*time.Second),
				randomDuration(10*time.Second),
				testIndexBlobDeleteAge+randomDuration(testIndexBlobDeleteAge),
				randomDuration(10*time.Second),
				randomDuration(2*testEventualConsistencySettleTime),
			)
		})
	}
}

func randomDuration(max time.Duration) time.Duration {
	return time.Duration(float64(max) * rand.Float64())
}

func verifyIndexBlobManagerPreventsResurrectOfDeletedContents(t *testing.T, delay1, delay2, delay3, delay4, delay5 time.Duration) {
	t.Logf("delays: %v %v %v %v %v", delay1, delay2, delay3, delay4, delay5)

	storageData := blobtesting.DataMap{}

	fakeTime := faketime.NewTimeAdvance(fakeLocalStartTime)
	fakeTimeFunc := fakeTime.NowFunc()

	st := blobtesting.NewMapStorage(storageData, nil, fakeTimeFunc)
	st = blobtesting.NewEventuallyConsistentStorage(st, testEventualConsistencySettleTime, fakeTimeFunc)
	st = logging.NewWrapper(st, func(msg string, args ...interface{}) {
		t.Logf("[store] "+fakeTimeFunc().Format("150405.000")+" "+msg, args...)
	}, "store: ")
	m := newIndexBlobManagerForTesting(t, st, fakeTimeFunc)

	numWritten := 0
	deleted := map[string]bool{}

	prefix := "prefix"
	ctx := testlogging.ContextWithLevelAndPrefixFunc(t, testlogging.LevelDebug, func() string {
		return fakeTimeFunc().Format("150405.000") + " "
	})

	// index#1 - write 2 contents
	must(t, writeFakeContents(ctx, m, prefix, 2, &numWritten, fakeTimeFunc))
	fakeTime.Advance(delay1)
	// index#2 - delete first of the two contents.
	must(t, deleteFakeContents(ctx, m, prefix, 1, deleted, fakeTimeFunc))
	fakeTime.Advance(delay2)
	// index#3, log#3 - replaces index#1 and #2
	must(t, fakeCompaction(ctx, m, true))
	fakeTime.Advance(delay3)

	numWritten2 := numWritten

	// index#4 - create one more content
	must(t, writeFakeContents(ctx, m, prefix, 2, &numWritten, fakeTimeFunc))
	fakeTime.Advance(delay4)

	// index#5, log#4 replaces index#3 and index#4, this will delete index#1 and index#2 and log#3
	must(t, fakeCompaction(ctx, m, true))

	t.Logf("************************************************ VERIFY")

	// advance the time just enough for eventual consistency to be visible
	fakeTime.Advance(delay5)

	// using another reader, make sure that all writes up to numWritten2 are correct regardless of whether
	// compaction is visible
	another := newIndexBlobManagerForTesting(t, st, fakeTimeFunc)
	must(t, verifyFakeContentsWritten(ctx, another, numWritten2, prefix, deleted))

	// verify that this reader can see all its own writes regardless of eventual consistency
	must(t, verifyFakeContentsWritten(ctx, m, numWritten, prefix, deleted))

	// after eventual consistency is settled, another reader can see all our writes
	fakeTime.Advance(testEventualConsistencySettleTime)
	must(t, verifyFakeContentsWritten(ctx, another, numWritten, prefix, deleted))
}

type fakeContentIndexEntry struct {
	ModTime time.Time
	Deleted bool
}

func verifyFakeContentsWritten(ctx context.Context, m indexBlobManager, numWritten int, contentPrefix string, deletedContents map[string]bool) error {
	if numWritten == 0 {
		return nil
	}

	log(ctx).Debugf("verifyFakeContentsWritten()")
	defer log(ctx).Debugf("finished verifyFakeContentsWritten()")

	all, _, err := getAllFakeContents(ctx, m)
	if err != nil {
		return errors.Wrap(err, "error getting all contents")
	}

	// verify that all contents previously written can be read.
	for i := 0; i < numWritten; i++ {
		id := fakeContentID(contentPrefix, i)
		if _, ok := all[id]; !ok {
			if deletedContents[id] {
				continue
			}

			return errors.Errorf("could not find content previously written by itself: %v (got %v)", id, all)
		}

		if got, want := all[id].Deleted, deletedContents[id]; got != want {
			return errors.Errorf("deleted flag does not match for %v: %v want %v", id, got, want)
		}
	}

	return nil
}

func fakeCompaction(ctx context.Context, m indexBlobManager, dropDeleted bool) error {
	log(ctx).Debugf("fakeCompaction(dropDeleted=%v)", dropDeleted)
	defer log(ctx).Debugf("finished fakeCompaction(dropDeleted=%v)", dropDeleted)

	allContents, allBlobs, err := getAllFakeContents(ctx, m)
	if err != nil {
		return errors.Wrap(err, "error getting contents")
	}

	dropped := map[string]fakeContentIndexEntry{}

	if dropDeleted {
		for cid, e := range allContents {
			if e.Deleted {
				dropped[cid] = e

				delete(allContents, cid)
			}
		}
	}

	if len(allBlobs) <= 1 {
		return nil
	}

	outputBM, err := writeFakeIndex(ctx, m, allContents)
	if err != nil {
		return errors.Wrap(err, "unable to write index")
	}

	for cid, e := range dropped {
		log(ctx).Debugf("dropped deleted %v %v from %v", cid, e, outputBM)
	}

	var (
		inputs  []blob.Metadata
		outputs = []blob.Metadata{outputBM}
	)

	for _, bi := range allBlobs {
		if bi.BlobID == outputBM.BlobID {
			// no compaction, output is the same as one of the inputs
			return nil
		}

		inputs = append(inputs, bi.Metadata)
	}

	if err := m.registerCompaction(ctx, inputs, outputs); err != nil {
		return errors.Wrap(err, "compaction error")
	}

	return nil
}

func fakeContentID(prefix string, n int) string {
	return fmt.Sprintf("%v-%06v", prefix, n)
}

func deleteFakeContents(ctx context.Context, m indexBlobManager, prefix string, numWritten int, deleted map[string]bool, timeFunc func() time.Time) error {
	if numWritten == 0 {
		return nil
	}

	log(ctx).Debugf("deleteFakeContents()")
	defer log(ctx).Debugf("finished deleteFakeContents()")

	count := rand.Intn(10) + 5

	ndx := map[string]fakeContentIndexEntry{}

	for i := 0; i < count; i++ {
		n := fakeContentID(prefix, rand.Intn(numWritten))
		if deleted[n] {
			continue
		}

		ndx[n] = fakeContentIndexEntry{
			ModTime: timeFunc(),
			Deleted: true,
		}

		deleted[n] = true
	}

	if len(ndx) == 0 {
		return nil
	}

	_, err := writeFakeIndex(ctx, m, ndx)

	return err
}

func undeleteFakeContents(ctx context.Context, m indexBlobManager, deleted map[string]bool, timeFunc func() time.Time) error {
	if len(deleted) == 0 {
		return nil
	}

	log(ctx).Debugf("undeleteFakeContents()")
	defer log(ctx).Debugf("finished undeleteFakeContents()")

	count := rand.Intn(5)

	ndx := map[string]fakeContentIndexEntry{}

	for n := range deleted {
		if count == 0 {
			break
		}

		// undelete
		ndx[n] = fakeContentIndexEntry{
			ModTime: timeFunc(),
			Deleted: false,
		}

		delete(deleted, n)
		count--
	}

	if len(ndx) == 0 {
		return nil
	}

	_, err := writeFakeIndex(ctx, m, ndx)

	return err
}

func writeFakeContents(ctx context.Context, m indexBlobManager, prefix string, count int, numWritten *int, timeFunc func() time.Time) error {
	log(ctx).Debugf("writeFakeContents()")
	defer log(ctx).Debugf("finished writeFakeContents()")

	ndx := map[string]fakeContentIndexEntry{}

	for i := 0; i < count; i++ {
		n := fakeContentID(prefix, *numWritten)
		ndx[n] = fakeContentIndexEntry{
			ModTime: timeFunc(),
		}

		(*numWritten)++
	}

	_, err := writeFakeIndex(ctx, m, ndx)

	return err
}

type fakeIndexData struct {
	RandomID int64
	Entries  map[string]fakeContentIndexEntry
}

func writeFakeIndex(ctx context.Context, m indexBlobManager, ndx map[string]fakeContentIndexEntry) (blob.Metadata, error) {
	j, err := json.Marshal(fakeIndexData{
		RandomID: rand.Int63(),
		Entries:  ndx,
	})
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "json error")
	}

	bm, err := m.writeIndexBlob(ctx, j)
	if err != nil {
		return blob.Metadata{}, errors.Wrap(err, "error writing blob")
	}

	for k, v := range ndx {
		log(ctx).Debugf("wrote content %v %v in blob %v", k, v, bm)
	}

	return bm, nil
}

var errGetAllFakeContentsRetry = errors.New("retry")

func getAllFakeContents(ctx context.Context, m indexBlobManager) (map[string]fakeContentIndexEntry, []IndexBlobInfo, error) {
	allContents, allBlobs, err := getAllFakeContentsInternal(ctx, m)

	for err == errGetAllFakeContentsRetry {
		allContents, allBlobs, err = getAllFakeContentsInternal(ctx, m)
	}

	return allContents, allBlobs, err
}

func getAllFakeContentsInternal(ctx context.Context, m indexBlobManager) (map[string]fakeContentIndexEntry, []IndexBlobInfo, error) {
	blobs, err := m.listIndexBlobs(ctx, false)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error listing index blobs")
	}

	log(ctx).Debugf("got blobs: %v", blobs)

	allContents := map[string]fakeContentIndexEntry{}

	for _, bi := range blobs {
		bb, err := m.getIndexBlob(ctx, bi.BlobID)
		if errors.Is(err, blob.ErrBlobNotFound) {
			return nil, nil, errGetAllFakeContentsRetry
		}

		if err != nil {
			return nil, nil, errors.Wrap(err, "error reading blob")
		}

		var indexData fakeIndexData

		if err := json.Unmarshal(bb, &indexData); err != nil {
			log(ctx).Debugf("invalid JSON %v: %v", string(bb), err)
			return nil, nil, errors.Wrap(err, "error unmarshaling")
		}

		// merge contents based based on time
		for k, v := range indexData.Entries {
			old, ok := allContents[k]

			if !ok {
				allContents[k] = v
			} else if v.ModTime.After(old.ModTime) {
				allContents[k] = v
			}
		}
	}

	return allContents, blobs, nil
}

func assertBlobCounts(t *testing.T, data blobtesting.DataMap, wantN, wantM, wantL int) {
	t.Helper()
	require.Len(t, keysWithPrefix(data, compactionLogBlobPrefix), wantM)
	require.Len(t, keysWithPrefix(data, indexBlobPrefix), wantN)
	require.Len(t, keysWithPrefix(data, "l"), wantL)
}

func keysWithPrefix(data blobtesting.DataMap, prefix blob.ID) []blob.ID {
	var res []blob.ID

	for k := range data {
		if strings.HasPrefix(string(k), string(prefix)) {
			res = append(res, k)
		}
	}

	return res
}

func mustRegisterCompaction(t *testing.T, m indexBlobManager, inputs, outputs []blob.Metadata) {
	t.Logf("compacting %v to %v", inputs, outputs)

	err := m.registerCompaction(testlogging.Context(t), inputs, outputs)
	if err != nil {
		t.Fatalf("failed to write index blob: %v", err)
	}
}

func mustWriteIndexBlob(t *testing.T, m indexBlobManager, data string) blob.Metadata {
	t.Logf("writing index blob %q", data)

	blobMD, err := m.writeIndexBlob(testlogging.Context(t), []byte(data))
	if err != nil {
		t.Fatalf("failed to write index blob: %v", err)
	}

	return blobMD
}

func assertIndexBlobList(t *testing.T, m indexBlobManager, wantMD ...blob.Metadata) {
	t.Helper()

	var want []blob.ID
	for _, it := range wantMD {
		want = append(want, it.BlobID)
	}

	l, err := m.listIndexBlobs(testlogging.Context(t), false)
	if err != nil {
		t.Fatalf("failed to list index blobs: %v", err)
	}

	t.Logf("asserting blob list %v vs %v", want, l)

	var got []blob.ID
	for _, it := range l {
		got = append(got, it.BlobID)
	}

	require.ElementsMatch(t, got, want)
}

func newIndexBlobManagerForTesting(t *testing.T, st blob.Storage, localTimeNow func() time.Time) indexBlobManager {
	p := &FormattingOptions{
		Encryption: encryption.DeprecatedNoneAlgorithm,
		Hash:       hashing.DefaultAlgorithm,
	}

	enc, err := encryption.CreateEncryptor(p)
	if err != nil {
		t.Fatalf("unable to create encryptor: %v", err)
	}

	hf, err := hashing.CreateHashFunc(p)
	if err != nil {
		t.Fatalf("unable to create hash: %v", err)
	}

	lc, err := newListCache(st, &CachingOptions{})
	if err != nil {
		t.Fatalf("unable to create list cache: %v", err)
	}

	m := &indexBlobManagerImpl{
		st: st,
		ownWritesCache: &persistentOwnWritesCache{
			blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, localTimeNow),
			localTimeNow,
		},
		indexBlobCache:                   passthroughContentCache{st},
		encryptor:                        enc,
		hasher:                           hf,
		listCache:                        lc,
		timeNow:                          localTimeNow,
		maxEventualConsistencySettleTime: testIndexBlobDeleteAge,
	}

	return m
}
