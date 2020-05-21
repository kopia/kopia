package content

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
	testIndexBlobDeleteAge         = 1 * time.Minute
	testCompactionLogBlobDeleteAge = 2 * time.Minute
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

			m := newIndexBlobManagerForTesting(t, storageData, fakeLocalTime.NowFunc(), fakeStorageTime.NowFunc())

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

func newIndexBlobManagerForTesting(t *testing.T, data blobtesting.DataMap, localTimeNow, storageTimeNow func() time.Time) indexBlobManager {
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

	st := blobtesting.NewMapStorage(data, nil, storageTimeNow)
	st = logging.NewWrapper(st, t.Logf, "STORE:")

	lc, err := newListCache(st, &CachingOptions{})
	if err != nil {
		t.Fatalf("unable to create list cache: %v", err)
	}

	m := &indexBlobManagerImpl{
		st: st,
		ownWritesCache: &persistentOwnWritesCache{
			logging.NewWrapper(blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, localTimeNow), t.Logf, "CACHE:"),
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
