package content

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
// to simulate clock drift
var (
	fakeLocalStartTime = time.Date(2020, 1, 1, 14, 0, 0, 0, time.UTC)
	fakeStoreStartTime = time.Date(2020, 1, 1, 10, 0, 0, 0, time.UTC)
)

const (
	testIndexBlobDeleteAge            = 1 * time.Minute
	testCompactionLogBlobDeleteAge    = 2 * time.Minute
	testEventualConsistencySettleTime = 45 * time.Second
)

func TestIndexBlobManager(t *testing.T) {
	cases := []struct {
		storageTimeAdvanceBetweenCompactions time.Duration
		wantCompactionLogCount               int
		wantIndexCount                       int
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
			// enough time has passed to delete 3 indexes but not compaction logs
			storageTimeAdvanceBetweenCompactions: testIndexBlobDeleteAge + 1*time.Second,
			wantIndexCount:                       3,
			wantCompactionLogCount:               2,
		},
		{
			// we write 6 index blobs and 2 compaction logs
			// enough time has passed to delete 3 indexes and 1 compaction log
			storageTimeAdvanceBetweenCompactions: testCompactionLogBlobDeleteAge + 1*time.Second,
			wantIndexCount:                       3,
			wantCompactionLogCount:               1,
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
			m := newIndexBlobManagerForTesting(t, st, fakeLocalTime.NowFunc(), fakeStorageTime.NowFunc())

			assetIndexBlobList(t, m)

			b1 := mustWriteIndexBlob(t, m, "index-1")
			assetIndexBlobList(t, m, b1)
			fakeStorageTime.Advance(1 * time.Second)

			b2 := mustWriteIndexBlob(t, m, "index-2")
			assetIndexBlobList(t, m, b1, b2)
			fakeStorageTime.Advance(1 * time.Second)

			b3 := mustWriteIndexBlob(t, m, "index-3")
			assetIndexBlobList(t, m, b1, b2, b3)
			fakeStorageTime.Advance(1 * time.Second)

			b4 := mustWriteIndexBlob(t, m, "index-4")
			assetIndexBlobList(t, m, b1, b2, b3, b4)
			fakeStorageTime.Advance(1 * time.Second)
			assertBlobCounts(t, storageData, 4, 0)

			// first compaction b1+b2+b3=>b4
			mustRegisterCompaction(t, m, []blob.Metadata{b1, b2, b3}, []blob.Metadata{b4})

			assetIndexBlobList(t, m, b4)
			fakeStorageTime.Advance(tc.storageTimeAdvanceBetweenCompactions)

			// second compaction b4+b5=>b6
			b5 := mustWriteIndexBlob(t, m, "index-5")
			b6 := mustWriteIndexBlob(t, m, "index-6")
			mustRegisterCompaction(t, m, []blob.Metadata{b4, b5}, []blob.Metadata{b6})
			assetIndexBlobList(t, m, b6)
			assertBlobCounts(t, storageData, tc.wantIndexCount, tc.wantCompactionLogCount)
		})
	}
}

type action int

const (
	actionWrite   = 1
	actionRead    = 2
	actionCompact = 3
	actionDelete  = 4
)

// actionsTestIndexBlobManagerStress is a set of actionsTestIndexBlobManagerStress by each actor performed in TestIndexBlobManagerStress with weights
var actionsTestIndexBlobManagerStress = []struct {
	a      action
	weight int
}{
	{actionWrite, 20},
	{actionRead, 60},
	{actionCompact, 10},
	{actionDelete, 10},
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
func TestIndexBlobManagerStress(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().UnixNano())

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

	numActors := 2 * runtime.NumCPU()

	for actorID := 0; actorID < numActors; actorID++ {
		actorID := actorID

		eg.Go(func() error {
			loggedSt := logging.NewWrapper(st, t.Logf, fmt.Sprintf("actor[%v]:", actorID))

			contentPrefix := fmt.Sprintf("a%v", actorID)
			numWritten := 0
			deletedContents := map[string]bool{}
			ctx := testlogging.ContextWithLevelAndPrefix(t, testlogging.LevelDebug, fmt.Sprintf("actor[%v]:", actorID))

			m := newIndexBlobManagerForTesting(t, loggedSt, fakeTimeFunc, fakeTimeFunc)

			// run stress test until the deadline, aborting early on any failure
			for fakeTimeFunc().Before(deadline) && time.Now().Before(localTimeDeadline) {
				switch pickRandomActionTestIndexBlobManagerStress() {
				case actionRead:
					if err := verifyFakeContentsWritten(ctx, m, numWritten, contentPrefix, deletedContents); err != nil {
						return errors.Wrapf(err, "actor[%v] error verifying contents", actorID)
					}

				case actionWrite:
					if err := writeFakeContents(ctx, m, contentPrefix, &numWritten, fakeTimeFunc); err != nil {
						return errors.Wrapf(err, "actor[%v] write error", actorID)
					}

				case actionDelete:
					if err := deleteFakeContents(ctx, m, contentPrefix, numWritten, deletedContents, fakeTimeFunc); err != nil {
						return errors.Wrapf(err, "actor[%v] delete error", actorID)
					}

				case actionCompact:
					if err := fakeCompaction(ctx, m); err != nil {
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

type fakeContentIndexEntry struct {
	ModTime time.Time
	Deleted bool
}

func verifyFakeContentsWritten(ctx context.Context, m indexBlobManager, numWritten int, contentPrefix string, deletedContents map[string]bool) error {
	all, _, err := getAllFakeContents(ctx, m)
	if err != nil {
		return errors.Wrap(err, "error getting all contents")
	}

	// verify that all contents previously written can be read.
	for i := 0; i < numWritten; i++ {
		id := fakeContentID(contentPrefix, i)
		if _, ok := all[id]; !ok {
			return errors.Errorf("could not find content previously written by itself: %v (got %v)", id, all)
		}

		if got, want := all[id].Deleted, deletedContents[id]; got != want {
			return errors.Errorf("deleted flag does not match for %v: %v want %v", id, got, want)
		}
	}

	return nil
}

func fakeCompaction(ctx context.Context, m indexBlobManager) error {
	log(ctx).Debugf("fakeCompaction()")
	defer log(ctx).Debugf("finished fakeCompaction()")

	allContents, allBlobs, err := getAllFakeContents(ctx, m)
	if err != nil {
		return errors.Wrap(err, "error getting contents")
	}

	if len(allBlobs) <= 1 {
		return nil
	}

	outputBM, err := writeFakeIndex(ctx, m, allContents)
	if err != nil {
		return errors.Wrap(err, "unable to write index")
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
	log(ctx).Debugf("deleteFakeContents()")
	defer log(ctx).Debugf("finished deleteFakeContents()")

	if numWritten == 0 {
		return nil
	}

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

func writeFakeContents(ctx context.Context, m indexBlobManager, prefix string, numWritten *int, timeFunc func() time.Time) error {
	log(ctx).Debugf("writeFakeContents()")
	defer log(ctx).Debugf("finished writeFakeContents()")

	count := rand.Intn(10) + 5

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

func writeFakeIndex(ctx context.Context, m indexBlobManager, ndx map[string]fakeContentIndexEntry) (blob.Metadata, error) {
	j, err := json.Marshal(ndx)
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

func getAllFakeContents(ctx context.Context, m indexBlobManager) (map[string]fakeContentIndexEntry, []IndexBlobInfo, error) {
	blobs, err := m.listIndexBlobs(ctx, false)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error listing index blobs")
	}

	log(ctx).Debugf("got blobs: %v", blobs)

	allContents := map[string]fakeContentIndexEntry{}

	for _, bi := range blobs {
		bb, err := m.getIndexBlob(ctx, bi.BlobID)
		if err == blob.ErrBlobNotFound {
			log(ctx).Debugf("ignoring NOT FOUND on %v", bi.BlobID)
			continue
		}

		if err != nil {
			return nil, nil, errors.Wrap(err, "error reading blob")
		}

		var contentIDs map[string]fakeContentIndexEntry
		if err := json.Unmarshal(bb, &contentIDs); err != nil {
			return nil, nil, errors.Wrap(err, "error unmarshaling")
		}

		// merge contents based based on time
		for k, v := range contentIDs {
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

func assertBlobCounts(t *testing.T, data blobtesting.DataMap, wantN, wantM int) {
	t.Helper()
	assert.Len(t, keysWithPrefix(data, compactionLogBlobPrefix), wantM)
	assert.Len(t, keysWithPrefix(data, indexBlobPrefix), wantN)
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

func assetIndexBlobList(t *testing.T, m indexBlobManager, wantMD ...blob.Metadata) {
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

	assert.ElementsMatch(t, got, want)
}

func newIndexBlobManagerForTesting(t *testing.T, st blob.Storage, localTimeNow, storageTimeNow func() time.Time) indexBlobManager {
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

	st = blobtesting.NewEventuallyConsistentStorage(st, testEventualConsistencySettleTime, storageTimeNow)

	lc, err := newListCache(st, &CachingOptions{})
	if err != nil {
		t.Fatalf("unable to create list cache: %v", err)
	}

	m := &indexBlobManagerImpl{
		st: st,
		ownWritesCache: &persistentOwnWritesCache{
			blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, localTimeNow),
			localTimeNow},
		indexBlobCache:                passthroughContentCache{st},
		encryptor:                     enc,
		hasher:                        hf,
		listCache:                     lc,
		timeNow:                       localTimeNow,
		minIndexBlobDeleteAge:         testIndexBlobDeleteAge,
		minCompactionLogBlobDeleteAge: testCompactionLogBlobDeleteAge,
	}

	return m
}
