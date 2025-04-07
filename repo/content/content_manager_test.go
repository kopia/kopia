package content

import (
	"bytes"
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/fault"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/indextest"
	"github.com/kopia/kopia/internal/ownwrites"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/format"
)

const (
	maxPackSize     = 2000
	maxPackCapacity = maxPackSize - defaultMaxPreambleLength
	maxRetries      = 100

	encryptionOverhead = 12 + 16
)

var (
	fakeTime   = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	hmacSecret = []byte{1, 2, 3}
)

func TestMain(m *testing.M) { testutil.MyTestMain(m) }

func TestFormatV1(t *testing.T) {
	testutil.RunAllTestsWithParam(t, &contentManagerSuite{
		mutableParameters: format.MutableParameters{
			Version:      1,
			IndexVersion: 1,
			MaxPackSize:  maxPackSize,
		},
	})
}

func TestFormatV2(t *testing.T) {
	testutil.RunAllTestsWithParam(t, &contentManagerSuite{
		mutableParameters: format.MutableParameters{
			Version:         2,
			MaxPackSize:     maxPackSize,
			IndexVersion:    index.Version2,
			EpochParameters: epoch.DefaultParameters(),
		},
	})
}

type contentManagerSuite struct {
	mutableParameters format.MutableParameters
}

func (s *contentManagerSuite) TestContentManagerEmptyFlush(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)
	bm.Flush(ctx)

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func (s *contentManagerSuite) TestContentZeroBytes1(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)
	contentID := writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	dumpContentManagerData(t, data)
	bm = s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	verifyContent(ctx, t, bm, contentID, []byte{})
}

func (s *contentManagerSuite) TestContentZeroBytes2(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	writeContentAndVerify(ctx, t, bm, seededRandomData(10, 10))
	writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
		dumpContentManagerData(t, data)
	}
}

func (s *contentManagerSuite) TestContentManagerSmallContentWrites(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	itemCount := maxPackCapacity / (10 + encryptionOverhead)
	for i := range itemCount {
		writeContentAndVerify(ctx, t, bm, seededRandomData(i, 10))
	}

	verifyBlobCount(t, data, map[blob.ID]int{"s": 1})
	bm.Flush(ctx)

	if s.mutableParameters.EpochParameters.Enabled {
		verifyBlobCount(t, data, map[blob.ID]int{"x": 1, "p": 1})
	} else {
		verifyBlobCount(t, data, map[blob.ID]int{"n": 1, "p": 1})
	}
}

func (s *contentManagerSuite) TestContentManagerDedupesPendingContents(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	for range 100 {
		writeContentAndVerify(ctx, t, bm, seededRandomData(0, maxPackCapacity/2))
	}

	// expect one blob which is a session marker.
	verifyBlobCount(t, data, map[blob.ID]int{"s": 1})

	bm.Flush(ctx)

	// session marker will be deleted and replaced with data + index.
	if s.mutableParameters.EpochParameters.Enabled {
		verifyBlobCount(t, data, map[blob.ID]int{"x": 1, "p": 1})
	} else {
		verifyBlobCount(t, data, map[blob.ID]int{"n": 1, "p": 1})
	}
}

func (s *contentManagerSuite) TestContentManagerDedupesPendingAndUncommittedContents(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	// compute content size so that 3 contents will fit in a pack without overflowing
	contentSize := maxPackCapacity/3 - encryptionOverhead - 1

	// no writes here, all data fits in a single pack.
	// but we will have a session marker.
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, contentSize))

	// expect one blob which is a session marker.
	verifyBlobCount(t, data, map[blob.ID]int{"s": 1})

	// no writes here
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, contentSize))

	// expect one blob which is a session marker.
	verifyBlobCount(t, data, map[blob.ID]int{"s": 1})

	bm.Flush(ctx)

	// this flushes the pack content + index blob and deletes session marker.
	if s.mutableParameters.EpochParameters.Enabled {
		verifyBlobCount(t, data, map[blob.ID]int{"x": 1, "p": 1})
	} else {
		verifyBlobCount(t, data, map[blob.ID]int{"n": 1, "p": 1})
	}
}

func (s *contentManagerSuite) TestContentManagerEmpty(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	noSuchContentID := hashValue(t, []byte("foo"))

	b, err := bm.GetContent(ctx, noSuchContentID)
	if !errors.Is(err, ErrContentNotFound) {
		t.Errorf("unexpected error when getting non-existent content: %v, %v", b, err)
	}

	bi, err := bm.ContentInfo(ctx, noSuchContentID)
	if !errors.Is(err, ErrContentNotFound) {
		t.Errorf("unexpected error when getting non-existent content info: %v, %v", bi, err)
	}

	verifyBlobCount(t, data, map[blob.ID]int{})
}

func verifyActiveIndexBlobCount(ctx context.Context, t *testing.T, bm *WriteManager, expected int) {
	t.Helper()

	blks, err := bm.IndexBlobs(ctx, false)
	if err != nil {
		t.Errorf("error listing active index blobs: %v", err)
		return
	}

	if got, want := len(blks), expected; got != want {
		t.Errorf("unexpected number of active index blobs %v, expected %v (%v)", got, want, blks)
	}
}

func (s *contentManagerSuite) TestContentManagerInternalFlush(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	itemsToOverflow := (maxPackCapacity)/(25+encryptionOverhead) + 2
	for range itemsToOverflow {
		b := make([]byte, 25)
		cryptorand.Read(b)
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 1 data blobs + session marker written, but no index yet.
	verifyBlobCount(t, data, map[blob.ID]int{"s": 1, "p": 1})

	// do it again - should be 2 blobs + some bytes pending.
	for range itemsToOverflow {
		b := make([]byte, 25)
		cryptorand.Read(b)
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 2 data blobs written + session marker, but no index yet.
	verifyBlobCount(t, data, map[blob.ID]int{"s": 1, "p": 2})

	bm.Flush(ctx)

	// third data blob gets written, followed by index, session marker gets deleted.
	if s.mutableParameters.EpochParameters.Enabled {
		verifyBlobCount(t, data, map[blob.ID]int{"x": 1, "p": 3})
	} else {
		verifyBlobCount(t, data, map[blob.ID]int{"n": 1, "p": 3})
	}
}

func (s *contentManagerSuite) TestContentManagerWriteMultiple(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	timeFunc := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, keyTime, timeFunc)

	bm := s.newTestContentManagerWithCustomTime(t, st, timeFunc)
	defer bm.CloseShared(ctx)

	var contentIDs []ID

	repeatCount := 5000
	if testutil.ShouldReduceTestComplexity() {
		repeatCount = 500
	}

	for i := range repeatCount {
		b := seededRandomData(i, i%113)

		blkID, err := bm.WriteContent(ctx, gather.FromSlice(b), "", NoCompression)
		if err != nil {
			t.Errorf("err: %v", err)
		}

		contentIDs = append(contentIDs, blkID)

		if i%17 == 0 {
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}
		}

		if i%41 == 0 {
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}

			bm = s.newTestContentManagerWithCustomTime(t, st, timeFunc)
			defer bm.CloseShared(ctx) //nolint:gocritic
		}

		pos := rand.Intn(len(contentIDs))
		if _, err := bm.GetContent(ctx, contentIDs[pos]); err != nil {
			dumpContentManagerData(t, data)
			t.Fatalf("can't read content %q: %v", contentIDs[pos], err)
		}
	}
}

// This is regression test for a bug where we would corrupt data when encryption
// was done in place and clobbered pending data in memory.
func (s *contentManagerSuite) TestContentManagerFailedToWritePack(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)
	faulty := blobtesting.NewFaultyStorage(st)
	st = faulty

	ta := faketime.NewTimeAdvance(fakeTime)

	bm, err := NewManagerForTesting(testlogging.Context(t), st, mustCreateFormatProvider(t, &format.ContentFormat{
		Hash:              "HMAC-SHA256-128",
		Encryption:        "AES256-GCM-HMAC-SHA256",
		MutableParameters: s.mutableParameters,
		HMACSecret:        []byte("foo"),
		MasterKey:         []byte("0123456789abcdef0123456789abcdef"),
	}), nil, &ManagerOptions{TimeNow: ta.NowFunc()})
	if err != nil {
		t.Fatalf("can't create bm: %v", err)
	}

	defer bm.CloseShared(ctx)

	sessionPutErr := errors.New("booboo0")
	firstPutErr := errors.New("booboo1")
	secondPutErr := errors.New("booboo2")

	faulty.AddFault(blobtesting.MethodPutBlob).ErrorInstead(sessionPutErr)
	faulty.AddFault(blobtesting.MethodPutBlob).ErrorInstead(firstPutErr)
	faulty.AddFault(blobtesting.MethodPutBlob).ErrorInstead(secondPutErr)

	_, err = bm.WriteContent(ctx, gather.FromSlice(seededRandomData(1, 10)), "", NoCompression)
	if !errors.Is(err, sessionPutErr) {
		t.Fatalf("can't create first content: %v", err)
	}

	b1, err := bm.WriteContent(ctx, gather.FromSlice(seededRandomData(1, 10)), "", NoCompression)
	if err != nil {
		t.Fatalf("can't create content: %v", err)
	}

	// advance time enough to cause auto-flush, which will fail (firstPutErr)
	ta.Advance(1 * time.Hour)

	if _, err := bm.WriteContent(ctx, gather.FromSlice(seededRandomData(2, 10)), "", NoCompression); !errors.Is(err, firstPutErr) {
		t.Fatalf("can't create 2nd content: %v", err)
	}

	// manual flush will fail because we're unable to write the blob (secondPutErr)
	if err := bm.Flush(ctx); !errors.Is(err, secondPutErr) {
		t.Logf("expected flush error: %v", err)
	}

	// flush will now succeed.
	if err := bm.Flush(ctx); err != nil {
		t.Logf("unexpected 2nd flush error: %v", err)
	}

	verifyContent(ctx, t, bm, b1, seededRandomData(1, 10))

	faulty.VerifyAllFaultsExercised(t)
}

func (s *contentManagerSuite) TestIndexCompactionDropsContent(t *testing.T) {
	if s.mutableParameters.EpochParameters.Enabled {
		t.Skip("dropping index entries not implemented")
	}

	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	timeFunc := faketime.AutoAdvance(fakeTime.Add(1), 1*time.Second)

	// create record in index #1
	st := blobtesting.NewMapStorage(data, keyTime, timeFunc)

	bm := s.newTestContentManagerWithCustomTime(t, st, timeFunc)
	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))

	timeFunc()

	// create record in index #2
	bm = s.newTestContentManagerWithCustomTime(t, st, timeFunc)
	deleteContent(ctx, t, bm, content1)
	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))

	timeFunc()

	deleteThreshold := timeFunc()

	t.Logf("----- compaction")

	bm = s.newTestContentManagerWithCustomTime(t, st, timeFunc)
	// this drops deleted entries, including from index #1
	require.NoError(t, bm.CompactIndexes(ctx, indexblob.CompactOptions{
		DropDeletedBefore: deleteThreshold,
		AllIndexes:        true,
	}))
	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))

	bm = s.newTestContentManagerWithCustomTime(t, st, timeFunc)
	verifyContentNotFound(ctx, t, bm, content1)
}

func (s *contentManagerSuite) TestContentManagerConcurrency(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	bm := s.newTestContentManagerWithCustomTime(t, st, nil)
	defer bm.CloseShared(ctx)

	preexistingContent := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)

	dumpContentManagerData(t, data)

	bm1 := s.newTestContentManager(t, st)
	defer bm1.CloseShared(ctx)

	bm2 := s.newTestContentManager(t, st)
	defer bm2.CloseShared(ctx)

	bm3 := s.newTestContentManagerWithCustomTime(t, st, faketime.AutoAdvance(fakeTime.Add(1), 1*time.Second))
	defer bm3.CloseShared(ctx)

	// all bm* can see pre-existing content
	verifyContent(ctx, t, bm1, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm2, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm3, preexistingContent, seededRandomData(10, 100))

	// write the same content in all managers.
	sharedContent := writeContentAndVerify(ctx, t, bm1, seededRandomData(20, 100))
	writeContentAndVerify(ctx, t, bm2, seededRandomData(20, 100))
	writeContentAndVerify(ctx, t, bm3, seededRandomData(20, 100))

	// write unique content per manager.
	bm1content := writeContentAndVerify(ctx, t, bm1, seededRandomData(31, 100))
	bm2content := writeContentAndVerify(ctx, t, bm2, seededRandomData(32, 100))
	bm3content := writeContentAndVerify(ctx, t, bm3, seededRandomData(33, 100))

	// make sure they can't see each other's unflushed contents.
	verifyContentNotFound(ctx, t, bm1, bm2content)
	verifyContentNotFound(ctx, t, bm1, bm3content)
	verifyContentNotFound(ctx, t, bm2, bm1content)
	verifyContentNotFound(ctx, t, bm2, bm3content)
	verifyContentNotFound(ctx, t, bm3, bm1content)
	verifyContentNotFound(ctx, t, bm3, bm2content)

	// now flush all writers, they still can't see each others' data.
	bm1.Flush(ctx)
	bm2.Flush(ctx)
	bm3.Flush(ctx)
	verifyContentNotFound(ctx, t, bm1, bm2content)
	verifyContentNotFound(ctx, t, bm1, bm3content)
	verifyContentNotFound(ctx, t, bm2, bm1content)
	verifyContentNotFound(ctx, t, bm2, bm3content)
	verifyContentNotFound(ctx, t, bm3, bm1content)
	verifyContentNotFound(ctx, t, bm3, bm2content)

	// new content manager at this point can see all data.
	bm4 := s.newTestContentManager(t, st)
	defer bm4.CloseShared(ctx)

	verifyContent(ctx, t, bm4, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm4, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm4, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm4, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm4, bm3content, seededRandomData(33, 100))

	validateIndexCount(t, data, 4, 0)

	if err := bm4.CompactIndexes(ctx, indexblob.CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("compaction error: %v", err)
	}

	if !s.mutableParameters.EpochParameters.Enabled {
		validateIndexCount(t, data, 5, 1)
	}

	// new content manager at this point can see all data.
	bm5 := s.newTestContentManager(t, st)
	defer bm5.CloseShared(ctx)

	verifyContent(ctx, t, bm5, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm5, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm5, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm5, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm5, bm3content, seededRandomData(33, 100))

	if err := bm5.CompactIndexes(ctx, indexblob.CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func validateIndexCount(t *testing.T, data map[blob.ID][]byte, wantIndexCount, wantCompactionLogCount int) {
	t.Helper()

	var indexCnt, compactionLogCnt int

	for blobID := range data {
		if strings.HasPrefix(string(blobID), indexblob.V0IndexBlobPrefix) || strings.HasPrefix(string(blobID), "x") {
			indexCnt++
		}

		if strings.HasPrefix(string(blobID), indexblob.V0CompactionLogBlobPrefix) {
			compactionLogCnt++
		}
	}

	if got, want := indexCnt, wantIndexCount; got != want {
		t.Fatalf("unexpected index blob count %v, want %v", got, want)
	}

	if got, want := compactionLogCnt, wantCompactionLogCount; got != want {
		t.Fatalf("unexpected compaction log blob count %v, want %v", got, want)
	}
}

func (s *contentManagerSuite) TestDeleteContent(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	defer bm.CloseShared(ctx)

	c1Bytes := seededRandomData(10, 100)
	content1 := writeContentAndVerify(ctx, t, bm, c1Bytes)

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("error flushing: %v", err)
	}

	dumpContents(ctx, t, bm, "after first flush")

	c2Bytes := seededRandomData(11, 100)
	content2 := writeContentAndVerify(ctx, t, bm, c2Bytes)

	t.Logf("deleting previously flushed content (c1)")

	if err := bm.DeleteContent(ctx, content1); err != nil {
		t.Fatalf("unable to delete content %v: %v", content1, err)
	}

	t.Logf("deleting not flushed content (c2)")

	if err := bm.DeleteContent(ctx, content2); err != nil {
		t.Fatalf("unable to delete content %v: %v", content2, err)
	}

	// c1 is readable, but should be marked as deleted at this point
	verifyDeletedContentRead(ctx, t, bm, content1, c1Bytes)
	verifyContentNotFound(ctx, t, bm, content2)
	t.Logf("flushing")
	bm.Flush(ctx)
	t.Logf("flushed")

	bm = s.newTestContentManager(t, st)
	defer bm.CloseShared(ctx)

	verifyDeletedContentRead(ctx, t, bm, content1, c1Bytes)
	verifyContentNotFound(ctx, t, bm, content2)
}

func (s *contentManagerSuite) TestDeletionAfterCreationWithFrozenTime(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)

	// first - write new content
	bm := s.newTestContentManagerWithCustomTime(t, st, faketime.Frozen(fakeTime))
	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(40, 16))
	require.NoError(t, bm.Flush(ctx))

	// second - delete content previously deleted
	bm = s.newTestContentManagerWithCustomTime(t, st, faketime.Frozen(fakeTime))
	ci, err := bm.ContentInfo(ctx, content1)
	require.NoError(t, err)
	require.Equal(t, fakeTime, ci.Timestamp().UTC())

	require.NoError(t, bm.DeleteContent(ctx, content1))
	require.NoError(t, bm.Flush(ctx))
	ci, err = bm.ContentInfo(ctx, content1)
	require.NoError(t, err)

	// time did not move, but we ensured that the time is greater than in the previous index.
	require.Equal(t, fakeTime.Add(1*time.Second), ci.Timestamp().UTC())

	// third - recreate content previously deleted
	bm = s.newTestContentManagerWithCustomTime(t, st, faketime.Frozen(fakeTime))
	require.Equal(t, content1, writeContentAndVerify(ctx, t, bm, seededRandomData(40, 16)))
	require.NoError(t, bm.Flush(ctx))

	ci, err = bm.ContentInfo(ctx, content1)
	require.NoError(t, err)

	// rewrite moves the time by another second
	require.Equal(t, fakeTime.Add(2*time.Second), ci.Timestamp().UTC())
}

//nolint:gocyclo
func (s *contentManagerSuite) TestUndeleteContentSimple(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(40, 16))
	content2 := writeContentAndVerify(ctx, t, bm, seededRandomData(41, 16))
	content3 := writeContentAndVerify(ctx, t, bm, seededRandomData(42, 16))

	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	dumpContents(ctx, t, bm, "after first flush")

	c1Info := getContentInfo(t, bm, content1)
	c2Info := getContentInfo(t, bm, content2)
	c3Info := getContentInfo(t, bm, content3)

	t.Log("deleting content 2: ", content2)
	deleteContent(ctx, t, bm, content2)

	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	t.Log("deleting content 3: ", content3)
	deleteContent(ctx, t, bm, content3)

	content4 := writeContentAndVerify(ctx, t, bm, seededRandomData(43, 16))

	t.Log("deleting content 4: ", content4)
	deleteContent(ctx, t, bm, content4)

	tcs := []struct {
		name    string
		cid     ID
		wantErr bool
		info    Info
	}{
		{
			name:    "existing content",
			cid:     content1,
			wantErr: false,
			info:    c1Info,
		},
		{
			name:    "flush after delete",
			cid:     content2,
			wantErr: false,
			info:    c2Info,
		},
		{
			name:    "no flush after delete",
			cid:     content3,
			wantErr: false,
			info:    c3Info,
		},
		{
			name:    "no flush after create and delete",
			cid:     content4,
			wantErr: true,
		},
		{
			name:    "non-existing content",
			cid:     makeRandomHexID(t, len(content3.String())), // non-existing
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Log("case name:", tc.name)

		err := bm.UndeleteContent(ctx, tc.cid)
		if got := err != nil; got != tc.wantErr {
			t.Errorf("did not get the expected error return valuem, want: %v, got: %v", tc.wantErr, err)
			continue
		}

		if tc.wantErr {
			continue
		}

		got, want := getContentInfo(t, bm, tc.cid), tc.info

		if got.Deleted {
			t.Error("Content marked as deleted:", got)
		}

		if got.PackBlobID == "" {
			t.Error("Empty pack id for undeleted content:", tc.cid)
		}

		if got.PackOffset == 0 {
			t.Error("0 offset for undeleted content:", tc.cid)
		}

		if diff := indextest.InfoDiff(want, got, "GetTimestampSeconds", "GetPackBlobID", "GetPackOffset", "Timestamp"); len(diff) > 0 {
			t.Fatalf("diff: %v", diff)
		}
	}

	// ensure content is still there after flushing
	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	tcs2 := []struct {
		name string
		cid  ID
		want Info
	}{
		{
			name: "content1",
			cid:  content1,
			want: c1Info,
		},
		{
			name: "content2",
			cid:  content2,
			want: c2Info,
		},
		{
			name: "content3",
			cid:  content3,
			want: c3Info,
		},
	}

	for _, tc := range tcs2 {
		t.Log("case name:", tc.name)
		got := getContentInfo(t, bm, tc.cid)

		if got.Deleted {
			t.Error("Content marked as deleted:", got)
		}

		if got.PackBlobID == "" {
			t.Error("Empty pack id for undeleted content:", tc.cid)
		}

		if got.PackOffset == 0 {
			t.Error("0 offset for undeleted content:", tc.cid)
		}

		// ignore different timestamps, pack id and pack offset
		if diff := indextest.InfoDiff(tc.want, got, "GetPackBlobID", "GetTimestampSeconds", "Timestamp"); len(diff) > 0 {
			t.Errorf("content info does not match. diff: %v", diff)
		}
	}
}

//nolint:gocyclo
func (s *contentManagerSuite) TestUndeleteContent(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	c1Bytes := seededRandomData(20, 10)
	content1 := writeContentAndVerify(ctx, t, bm, c1Bytes)

	c2Bytes := seededRandomData(21, 10)
	content2 := writeContentAndVerify(ctx, t, bm, c2Bytes)
	content3 := writeContentAndVerify(ctx, t, bm, seededRandomData(31, 10))

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("error flushing: %v", err)
	}

	dumpContents(ctx, t, bm, "after first flush")

	t.Logf("deleting content 1: %s", content1)

	if err := bm.DeleteContent(ctx, content1); err != nil {
		t.Fatalf("unable to delete content %v: %v", content1, err)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("error flushing: %v", err)
	}

	t.Logf("deleting content 2: %s", content2)

	if err := bm.DeleteContent(ctx, content2); err != nil {
		t.Fatalf("unable to delete content %v: %v", content2, err)
	}

	content4 := writeContentAndVerify(ctx, t, bm, seededRandomData(41, 10))
	content5 := writeContentAndVerify(ctx, t, bm, seededRandomData(51, 10))

	t.Logf("deleting content 4: %s", content4)

	if err := bm.DeleteContent(ctx, content4); err != nil {
		t.Fatalf("unable to delete content %v: %v", content4, err)
	}

	verifyDeletedContentRead(ctx, t, bm, content1, c1Bytes)
	verifyDeletedContentRead(ctx, t, bm, content2, c2Bytes)
	verifyContentNotFound(ctx, t, bm, content4)

	// At this point:
	// - content 1 is flushed, deleted index entry has been flushed
	// - content 2 is flushed, deleted index entry has not been flushed
	// - content 3 is flushed, not deleted
	// - content 4 is not flushed and deleted, it cannot be undeleted
	// - content 5 is not flushed and not deleted

	if err := bm.UndeleteContent(ctx, content1); err != nil {
		t.Fatal("unable to undelete content 1: ", content1, err)
	}

	if err := bm.UndeleteContent(ctx, content2); err != nil {
		t.Fatal("unable to undelete content 2: ", content2, err)
	}

	if err := bm.UndeleteContent(ctx, content3); err != nil {
		t.Fatal("unable to undelete content 3: ", content3, err)
	}

	if err := bm.UndeleteContent(ctx, content4); err == nil {
		t.Fatal("was able to undelete content 4: ", content4)
	}

	if err := bm.UndeleteContent(ctx, content5); err != nil {
		t.Fatal("unable to undelete content 5: ", content5, err)
	}

	// verify content is not marked as deleted
	for _, id := range []ID{} {
		ci, err := bm.ContentInfo(ctx, id)
		if err != nil {
			t.Fatalf("unable to get content info for %v: %v", id, err)
		}

		if got, want := ci.Deleted, false; got != want {
			t.Fatalf("content %v was not undeleted: %v", id, ci)
		}
	}

	t.Logf("flushing ...")
	bm.Flush(ctx)
	t.Logf("... flushed")

	// verify content is not marked as deleted
	for _, id := range []ID{} {
		ci, err := bm.ContentInfo(ctx, id)
		if err != nil {
			t.Fatalf("unable to get content info for %v: %v", id, err)
		}

		if got, want := ci.Deleted, false; got != want {
			t.Fatalf("content %v was not undeleted: %v", id, ci)
		}
	}

	bm = s.newTestContentManager(t, st)
	verifyContentNotFound(ctx, t, bm, content4)

	// verify content is not marked as deleted
	for _, id := range []ID{} {
		ci, err := bm.ContentInfo(ctx, id)
		if err != nil {
			t.Fatalf("unable to get content info for %v: %v", id, err)
		}

		if got, want := ci.Deleted, false; got != want {
			t.Fatalf("content %v was not undeleted: %v", id, ci)
		}
	}
}

func (s *contentManagerSuite) TestDeleteAfterUndelete(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(40, 16))
	content2 := writeContentAndVerify(ctx, t, bm, seededRandomData(41, 16))

	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	dumpContents(ctx, t, bm, "after first flush")

	deleteContent(ctx, t, bm, content1)
	deleteContent(ctx, t, bm, content2)

	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	c1Want := getContentInfo(t, bm, content1)

	// undelete, delete, check, flush, check
	if err := bm.UndeleteContent(ctx, content1); err != nil {
		t.Fatal("unable to undelete content 1: ", content1, err)
	}

	// undelete, flush, delete, check, flush, check
	if err := bm.UndeleteContent(ctx, content2); err != nil {
		t.Fatal("unable to undelete content 2: ", content2, err)
	}

	c2Want := getContentInfo(t, bm, content2)

	// delete content1 before flushing
	deleteContentAfterUndeleteAndCheck(ctx, t, bm, content1, c1Want)

	// now delete c2 after having flushed
	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	c2Want = withDeleted(c2Want)
	deleteContentAfterUndeleteAndCheck(ctx, t, bm, content2, c2Want)
}

func deleteContentAfterUndeleteAndCheck(ctx context.Context, t *testing.T, bm *WriteManager, id ID, want Info) {
	t.Helper()
	deleteContent(ctx, t, bm, id)

	got := getContentInfo(t, bm, id)
	if !got.Deleted {
		t.Fatalf("Expected content %q to be deleted, got: %#v", id, got)
	}

	if diff := indextest.InfoDiff(want, got, "GetTimestampSeconds", "Timestamp"); len(diff) != 0 {
		t.Fatalf("Content %q info does not match\ndiff: %v", id, diff)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	// check c1 again
	got = getContentInfo(t, bm, id)
	if !got.Deleted {
		t.Fatal("Expected content to be deleted, got: ", got)
	}

	// ignore timestamp
	if diff := indextest.InfoDiff(want, got, "GetTimestampSeconds", "Timestamp"); len(diff) != 0 {
		t.Fatalf("Content info does not match\ndiff: %v", diff)
	}
}

func (s *contentManagerSuite) TestParallelWrites(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		return
	}

	ctx := testlogging.Context(t)

	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	// set up fake storage that is slow at PutBlob causing writes to be piling up
	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodPutBlob).Repeat(1000000000).SleepFor(1 * time.Second)

	var workersWG sync.WaitGroup

	var workerLock sync.RWMutex

	bm := s.newTestContentManagerWithTweaks(t, fs, nil)
	defer bm.CloseShared(ctx)

	numWorkers := 8
	closeWorkers := make(chan bool)

	// workerLock allows workers to append to their own list of IDs (when R-locked) in parallel.
	// W-lock allows flusher to capture the state without any worker being able to modify it.
	workerWritten := make([][]ID, numWorkers)

	// start numWorkers, each writing random block and recording it
	for workerID := range numWorkers {
		workersWG.Add(1)

		go func() {
			defer workersWG.Done()

			for {
				select {
				case <-closeWorkers:
					return
				case <-time.After(1 * time.Nanosecond):
					id := writeContentAndVerify(ctx, t, bm, seededRandomData(rand.Int(), 100))

					workerLock.RLock()
					workerWritten[workerID] = append(workerWritten[workerID], id)
					workerLock.RUnlock()
				}
			}
		}()
	}

	closeFlusher := make(chan bool)

	var flusherWG sync.WaitGroup

	flusherWG.Add(1)

	go func() {
		defer flusherWG.Done()

		for {
			select {
			case <-closeFlusher:
				t.Logf("closing flusher goroutine")
				return
			case <-time.After(2 * time.Second):
				t.Logf("about to flush")

				// capture snapshot of all content IDs while holding a writer lock
				allWritten := map[ID]bool{}

				workerLock.Lock()

				for _, ww := range workerWritten {
					for _, id := range ww {
						allWritten[id] = true
					}
				}

				workerLock.Unlock()

				t.Logf("captured %v contents", len(allWritten))

				if err := bm.Flush(ctx); err != nil {
					t.Errorf("flush error: %v", err)
				}

				// open new content manager and verify all contents are visible there.
				s.verifyAllDataPresent(ctx, t, st, allWritten)
			}
		}
	}()

	// run workers and flushers for some time, enough for 2 flushes to complete
	time.Sleep(5 * time.Second)

	// shut down workers and wait for them
	close(closeWorkers)
	workersWG.Wait()

	close(closeFlusher)
	flusherWG.Wait()
}

func (s *contentManagerSuite) TestFlushResumesWriters(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	resumeWrites := make(chan struct{})

	// set up fake storage that is slow at PutBlob causing writes to be piling up
	fs := blobtesting.NewFaultyStorage(st)
	fs.AddFault(blobtesting.MethodPutBlob).ErrorCallbackInstead(func() error {
		close(resumeWrites)
		return nil
	})

	bm := s.newTestContentManagerWithTweaks(t, fs, nil)
	defer bm.CloseShared(ctx)
	first := writeContentAndVerify(ctx, t, bm, []byte{1, 2, 3})

	var second ID

	var writeWG sync.WaitGroup

	writeWG.Add(1)

	go func() {
		defer writeWG.Done()

		// start a write while flush is ongoing, the write will block on the condition variable
		<-resumeWrites
		t.Logf("write started")

		second = writeContentAndVerify(ctx, t, bm, []byte{3, 4, 5})

		t.Logf("write finished")
	}()

	// flush will take 5 seconds, 1 second into that we will start a write
	bm.Flush(ctx)

	// wait for write to complete, if this times out, Flush() is not waking up writers
	writeWG.Wait()

	s.verifyAllDataPresent(ctx, t, st, map[ID]bool{
		first: true,
	})

	// flush again, this will include buffer
	bm.Flush(ctx)

	s.verifyAllDataPresent(ctx, t, st, map[ID]bool{
		first:  true,
		second: true,
	})
}

func (s *contentManagerSuite) TestFlushWaitsForAllPendingWriters(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	fs := blobtesting.NewFaultyStorage(st)

	// first write is fast (session ID blobs)
	fs.AddFault(blobtesting.MethodPutBlob)
	// second write is slow
	fs.AddFault(blobtesting.MethodPutBlob).SleepFor(2 * time.Second)

	bm := s.newTestContentManagerWithTweaks(t, fs, nil)
	defer bm.CloseShared(ctx)

	// write one content in another goroutine
	// 'fs' is configured so that blob write takes several seconds to complete.
	go writeContentAndVerify(ctx, t, bm, seededRandomData(1, maxPackSize))

	// wait enough time for the goroutine to start writing.
	time.Sleep(100 * time.Millisecond)

	// write second short content
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, maxPackSize/4))

	// flush will wait for both writes to complete.
	t.Logf(">>> start of flushing")
	bm.Flush(ctx)
	t.Logf("<<< end of flushing")

	indexBlobPrefix := blob.ID(indexblob.V0IndexBlobPrefix)
	if s.mutableParameters.EpochParameters.Enabled {
		indexBlobPrefix = "x"
	}

	verifyBlobCount(t, data, map[blob.ID]int{
		PackBlobIDPrefixRegular: 2,
		indexBlobPrefix:         1,
	})

	bm.Flush(ctx)

	verifyBlobCount(t, data, map[blob.ID]int{
		PackBlobIDPrefixRegular: 2,
		indexBlobPrefix:         1,
	})
}

func (s *contentManagerSuite) verifyAllDataPresent(ctx context.Context, t *testing.T, st blob.Storage, contentIDs map[ID]bool) {
	t.Helper()

	bm := s.newTestContentManagerWithCustomTime(t, st, nil)
	defer bm.CloseShared(ctx)

	_ = bm.IterateContents(ctx, IterateOptions{}, func(ci Info) error {
		delete(contentIDs, ci.ContentID)
		return nil
	})

	if len(contentIDs) != 0 {
		t.Errorf("some blocks not written: %v", contentIDs)
	}
}

func (s *contentManagerSuite) TestHandleWriteErrors(t *testing.T) {
	// genFaults(S0,F0,S1,F1,...,) generates a list of faults
	// where success is returned Sn times followed by failure returned Fn times
	genFaults := func(counts ...int) []*fault.Fault {
		var result []*fault.Fault

		for i, cnt := range counts {
			if i%2 == 0 {
				if cnt > 0 {
					result = append(result, fault.New().Repeat(cnt-1))
				}
			} else {
				if cnt > 0 {
					result = append(result, fault.New().Repeat(cnt-1).ErrorInstead(errors.New("some write error")))
				}
			}
		}

		return result
	}

	// simulate a stream of PutBlob failures, write some contents followed by flush
	// count how many times we retried writes/flushes
	// also, verify that all the data is durable
	cases := []struct {
		faults               []*fault.Fault // failures to similuate
		contentSizes         []int          // sizes of contents to write
		expectedWriteRetries []int
		expectedFlushRetries int
	}{
		// write 3 packs of maxPackSize
		// PutBlob: {1 x SUCCESS (session marker), 5 x FAILURE, 3 x SUCCESS, 9 x FAILURE }
		{faults: genFaults(1, 5, 3, 9), contentSizes: []int{maxPackSize, maxPackSize, maxPackSize}, expectedWriteRetries: []int{5, 0, 0}, expectedFlushRetries: 9},

		// write 1 content which succeeds, then flush which will fail 5 times before succeeding.
		{faults: genFaults(2, 5), contentSizes: []int{maxPackSize}, expectedWriteRetries: []int{0}, expectedFlushRetries: 5},

		// write 4 contents, first write succeeds, next one fails 7 times, then all successes.
		{faults: genFaults(2, 7), contentSizes: []int{maxPackSize, maxPackSize, maxPackSize, maxPackSize}, expectedWriteRetries: []int{0, 7, 0, 0}, expectedFlushRetries: 0},

		// first flush fill fail on pack write, next 3 will fail on index writes.
		{faults: genFaults(1, 1, 0, 3), contentSizes: []int{maxPackSize / 2}, expectedWriteRetries: []int{0}, expectedFlushRetries: 4},

		// second write will be retried 5 times, flush will be retried 3 times.
		{faults: genFaults(1, 5, 1, 3), contentSizes: []int{maxPackSize / 2, maxPackSize / 2}, expectedWriteRetries: []int{0, 5}, expectedFlushRetries: 3},
	}

	for n, tc := range cases {
		t.Run(fmt.Sprintf("case-%v", n), func(t *testing.T) {
			ctx := testlogging.Context(t)
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}
			st := blobtesting.NewMapStorage(data, keyTime, nil)

			// set up fake storage that is slow at PutBlob causing writes to be piling up
			fs := blobtesting.NewFaultyStorage(st)
			fs.AddFaults(blobtesting.MethodPutBlob, tc.faults...)

			bm := s.newTestContentManagerWithTweaks(t, fs, nil)
			defer bm.CloseShared(ctx)

			var writeRetries []int
			var cids []ID
			for i, size := range tc.contentSizes {
				t.Logf(">>>> writing %v", i)
				cid, retries := writeContentWithRetriesAndVerify(ctx, t, bm, seededRandomData(i, size))
				writeRetries = append(writeRetries, retries)
				cids = append(cids, cid)
			}
			if got, want := flushWithRetries(ctx, t, bm), tc.expectedFlushRetries; got != want {
				t.Fatalf("invalid # of flush retries %v, wanted %v", got, want)
			}
			if diff := cmp.Diff(writeRetries, tc.expectedWriteRetries); diff != "" {
				t.Fatalf("invalid # of write retries (-got,+want): %v", diff)
			}

			bm2 := s.newTestContentManagerWithTweaks(t, st, nil)
			defer bm2.CloseShared(ctx)

			for i, cid := range cids {
				verifyContent(ctx, t, bm2, cid, seededRandomData(i, tc.contentSizes[i]))
			}

			fs.VerifyAllFaultsExercised(t)
		})
	}
}

func (s *contentManagerSuite) TestRewriteNonDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> RewriteContent() <action2> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := range stepBehaviors {
		for action2 := range stepBehaviors {
			t.Run(fmt.Sprintf("case-%v-%v", action1, action2), func(t *testing.T) {
				ctx := testlogging.Context(t)
				data := blobtesting.DataMap{}
				keyTime := map[blob.ID]time.Time{}
				fakeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
				st := blobtesting.NewMapStorage(data, keyTime, fakeNow)

				bm := s.newTestContentManagerWithCustomTime(t, st, fakeNow)
				defer bm.CloseShared(ctx)

				applyStep := func(action int) {
					switch action {
					case 0:
						t.Logf("flushing and reopening")
						bm.Flush(ctx)
						bm = s.newTestContentManagerWithCustomTime(t, st, fakeNow)
						defer bm.CloseShared(ctx)
					case 1:
						t.Logf("flushing")
						bm.Flush(ctx)
					case 2:
						t.Logf("doing nothing")
					}
				}

				content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
				applyStep(action1)
				require.NoError(t, bm.RewriteContent(ctx, content1))
				applyStep(action2)
				verifyContent(ctx, t, bm, content1, seededRandomData(10, 100))
				dumpContentManagerData(t, data)
			})
		}
	}
}

func (s *contentManagerSuite) TestDisableFlush(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	bm.DisableIndexFlush(ctx)
	bm.DisableIndexFlush(ctx)

	for i := range 500 {
		writeContentAndVerify(ctx, t, bm, seededRandomData(i, 100))
	}
	bm.Flush(ctx) // flush will not have effect
	bm.EnableIndexFlush(ctx)
	bm.Flush(ctx) // flush will not have effect
	bm.EnableIndexFlush(ctx)

	verifyActiveIndexBlobCount(ctx, t, bm, 0)
	bm.EnableIndexFlush(ctx)
	verifyActiveIndexBlobCount(ctx, t, bm, 0)
	bm.Flush(ctx) // flush will happen now
	verifyActiveIndexBlobCount(ctx, t, bm, 1)
}

func (s *contentManagerSuite) TestRewriteDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> Delete() <action2> RewriteContent() <action3> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := range stepBehaviors {
		for action2 := range stepBehaviors {
			for action3 := range stepBehaviors {
				t.Run(fmt.Sprintf("case-%v-%v-%v", action1, action2, action3), func(t *testing.T) {
					ctx := testlogging.Context(t)
					data := blobtesting.DataMap{}
					keyTime := map[blob.ID]time.Time{}
					fakeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
					st := blobtesting.NewMapStorage(data, keyTime, fakeNow)
					bm := s.newTestContentManagerWithCustomTime(t, st, fakeNow)
					defer bm.CloseShared(ctx)

					applyStep := func(action int) {
						switch action {
						case 0:
							t.Logf("flushing and reopening")
							bm.Flush(ctx)
							bm = s.newTestContentManagerWithCustomTime(t, st, fakeNow)
							defer bm.CloseShared(ctx)
						case 1:
							t.Logf("flushing")
							bm.Flush(ctx)
						case 2:
							t.Logf("doing nothing")
						}
					}

					c1Bytes := seededRandomData(10, 100)
					content1 := writeContentAndVerify(ctx, t, bm, c1Bytes)
					applyStep(action1)
					require.NoError(t, bm.DeleteContent(ctx, content1))
					applyStep(action2)

					if got, want := bm.RewriteContent(ctx, content1), ErrContentNotFound; !errors.Is(got, want) && got != nil {
						t.Errorf("unexpected error %v, wanted %v", got, want)
					}
					applyStep(action3)
					if action1 == 2 { // no flush
						verifyContentNotFound(ctx, t, bm, content1)
					} else {
						verifyDeletedContentRead(ctx, t, bm, content1, c1Bytes)
					}
					dumpContentManagerData(t, data)
				})
			}
		}
	}
}

func (s *contentManagerSuite) TestDeleteAndRecreate(t *testing.T) {
	ctx := testlogging.Context(t)
	// simulate race between delete/recreate and
	// delete happens at t0+10, recreate at t0+20 and second delete time is parameterized.
	// depending on it, the second delete results will be visible.
	cases := []struct {
		desc         string
		deletionTime time.Time
		isVisible    bool
	}{
		{"deleted before delete and-recreate", fakeTime.Add(5 * time.Second), true},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// write a content
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}

			st := blobtesting.NewMapStorage(data, keyTime, faketime.Frozen(fakeTime))

			bm := s.newTestContentManagerWithCustomTime(t, st, faketime.Frozen(fakeTime))
			defer bm.CloseShared(ctx)

			content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
			bm.Flush(ctx)

			// delete but at given timestamp but don't commit yet.
			bm0 := s.newTestContentManagerWithCustomTime(t, st, faketime.AutoAdvance(tc.deletionTime, 1*time.Second))
			defer bm0.CloseShared(ctx)

			require.NoError(t, bm0.DeleteContent(ctx, content1))

			// delete it at t0+10
			bm1 := s.newTestContentManagerWithCustomTime(t, st, faketime.AutoAdvance(fakeTime.Add(10*time.Second), 1*time.Second))
			defer bm1.CloseShared(ctx)

			verifyContent(ctx, t, bm1, content1, seededRandomData(10, 100))
			require.NoError(t, bm1.DeleteContent(ctx, content1))
			bm1.Flush(ctx)

			// recreate at t0+20
			bm2 := s.newTestContentManagerWithCustomTime(t, st, faketime.AutoAdvance(fakeTime.Add(20*time.Second), 1*time.Second))
			defer bm2.CloseShared(ctx)

			content2 := writeContentAndVerify(ctx, t, bm2, seededRandomData(10, 100))
			bm2.Flush(ctx)

			// commit deletion from bm0 (t0+5)
			bm0.Flush(ctx)

			if content1 != content2 {
				t.Errorf("got invalid content %v, expected %v", content2, content1)
			}

			bm3 := s.newTestContentManager(t, st)
			defer bm3.CloseShared(ctx)

			dumpContentManagerData(t, data)
			if tc.isVisible {
				verifyContent(ctx, t, bm3, content1, seededRandomData(10, 100))
			} else {
				verifyContentNotFound(ctx, t, bm3, content1)
			}
		})
	}
}

func (s *contentManagerSuite) TestIterateContents(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	// flushed, non-deleted
	contentID1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))

	// flushed, deleted
	contentID2 := writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))
	bm.Flush(ctx)

	if err := bm.DeleteContent(ctx, contentID2); err != nil {
		t.Errorf("error deleting content 2 %v", err)
	}

	// pending, non-deleted
	contentID3 := writeContentAndVerify(ctx, t, bm, seededRandomData(12, 100))

	// pending, deleted - is completely discarded
	contentID4 := writeContentAndVerify(ctx, t, bm, seededRandomData(13, 100))
	if err := bm.DeleteContent(ctx, contentID4); err != nil {
		t.Fatalf("error deleting content 4 %v", err)
	}

	t.Logf("contentID1: %v", contentID1)
	t.Logf("contentID2: %v", contentID2)
	t.Logf("contentID3: %v", contentID3)
	t.Logf("contentID4: %v", contentID4)

	someError := errors.New("some error")
	cases := []struct {
		desc    string
		options IterateOptions
		want    map[ID]bool
		sleep   time.Duration
		fail    error
	}{
		{
			desc:    "default options",
			options: IterateOptions{},
			want:    map[ID]bool{contentID1: true, contentID3: true},
		},
		{
			desc:    "include deleted",
			options: IterateOptions{IncludeDeleted: true},
			want: map[ID]bool{
				contentID1: true,
				contentID2: true,
				contentID3: true,
			},
		},
		{
			desc: "parallel",
			options: IterateOptions{
				Parallel: 10,
			},
			want: map[ID]bool{
				contentID1: true,
				contentID3: true,
			},
		},
		{
			desc:    "failure",
			options: IterateOptions{},
			fail:    someError,
			want:    map[ID]bool{},
		},
		{
			desc: "failure-parallel",
			options: IterateOptions{
				Parallel: 10,
			},
			fail:  someError,
			sleep: 10 * time.Millisecond,
			want:  map[ID]bool{},
		},
		{
			desc: "parallel, include deleted",
			options: IterateOptions{
				Parallel:       10,
				IncludeDeleted: true,
			},
			want: map[ID]bool{
				contentID1: true,
				contentID2: true,
				contentID3: true,
			},
		},
		{
			desc: "prefix match",
			options: IterateOptions{
				Range: index.PrefixRange(index.IDPrefix(contentID1.String())),
			},
			want: map[ID]bool{contentID1: true},
		},
		{
			desc: "prefix, include deleted",
			options: IterateOptions{
				Range:          index.PrefixRange(index.IDPrefix(contentID2.String())),
				IncludeDeleted: true,
			},
			want: map[ID]bool{
				contentID2: true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			var mu sync.Mutex
			got := map[ID]bool{}

			err := bm.IterateContents(ctx, tc.options, func(ci Info) error {
				if tc.sleep > 0 {
					time.Sleep(tc.sleep)
				}

				if tc.fail != nil {
					return tc.fail
				}

				mu.Lock()
				got[ci.ContentID] = true
				mu.Unlock()
				return nil
			})

			if !errors.Is(err, tc.fail) {
				t.Errorf("error iterating: %v", err)
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("invalid content IDs got: %v, want %v", got, tc.want)
			}
		})
	}
}

func (s *contentManagerSuite) TestFindUnreferencedBlobs(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	verifyUnreferencedBlobsCount(ctx, t, bm, 0)
	contentID := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))

	t.Logf("flushing")

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(ctx, t, bm, "after flush #1")
	dumpContentManagerData(t, data)
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	if err := bm.DeleteContent(ctx, contentID); err != nil {
		t.Errorf("error deleting content: %v", contentID)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(ctx, t, bm, "after flush #2")
	dumpContentManagerData(t, data)
	// content still present in first pack
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	require.NoError(t, bm.RewriteContent(ctx, contentID))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	verifyUnreferencedBlobsCount(ctx, t, bm, 1)
	require.NoError(t, bm.RewriteContent(ctx, contentID))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	verifyUnreferencedBlobsCount(ctx, t, bm, 2)
}

func (s *contentManagerSuite) TestFindUnreferencedBlobs2(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	verifyUnreferencedBlobsCount(ctx, t, bm, 0)
	contentID := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))
	dumpContents(ctx, t, bm, "after writing")

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(ctx, t, bm, "after flush")
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	if err := bm.DeleteContent(ctx, contentID); err != nil {
		t.Errorf("error deleting content: %v", contentID)
	}

	dumpContents(ctx, t, bm, "after delete")

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(ctx, t, bm, "after flush")
	// content present in first pack, original pack is still referenced
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)
}

func dumpContents(ctx context.Context, t *testing.T, bm *WriteManager, caption string) {
	t.Helper()

	count := 0

	t.Logf("dumping %v contents", caption)

	if err := bm.IterateContents(ctx, IterateOptions{IncludeDeleted: true},
		func(ci Info) error {
			t.Logf(" ci[%v]=%#v", count, ci)
			count++
			return nil
		}); err != nil {
		t.Errorf("error listing contents: %v", err)
		return
	}

	t.Logf("finished dumping %v %v contents", count, caption)
}

func verifyUnreferencedBlobsCount(ctx context.Context, t *testing.T, bm *WriteManager, want int) {
	t.Helper()

	var unrefCount int32

	err := bm.IterateUnreferencedBlobs(ctx, nil, 1, func(_ blob.Metadata) error {
		atomic.AddInt32(&unrefCount, 1)
		return nil
	})
	if err != nil {
		t.Errorf("error in IterateUnreferencedBlobs: %v", err)
	}

	t.Logf("got %v expecting %v", unrefCount, want)

	if got := int(unrefCount); got != want {
		t.Fatalf("invalid number of unreferenced contents: %v, wanted %v", got, want)
	}
}

func (s *contentManagerSuite) TestContentWriteAliasing(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, faketime.Frozen(fakeTime))

	bm := s.newTestContentManagerWithCustomTime(t, st, faketime.Frozen(fakeTime))
	defer bm.CloseShared(ctx)

	contentData := []byte{100, 0, 0}
	id1 := writeContentAndVerify(ctx, t, bm, contentData)
	contentData[0] = 101
	id2 := writeContentAndVerify(ctx, t, bm, contentData)
	bm.Flush(ctx)

	contentData[0] = 102

	id3 := writeContentAndVerify(ctx, t, bm, contentData)

	contentData[0] = 103

	id4 := writeContentAndVerify(ctx, t, bm, contentData)

	verifyContent(ctx, t, bm, id1, []byte{100, 0, 0})
	verifyContent(ctx, t, bm, id2, []byte{101, 0, 0})
	verifyContent(ctx, t, bm, id3, []byte{102, 0, 0})
	verifyContent(ctx, t, bm, id4, []byte{103, 0, 0})
}

func (s *contentManagerSuite) TestDisableCompressionOfMetadata(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManager(t, st)

	//nolint:lll
	contentID, err := bm.WriteContent(ctx,
		dirMetadataContent(),
		"k",
		NoCompression)
	require.NoError(t, err)

	info, err := bm.ContentInfo(ctx, contentID)
	require.NoError(t, err)
	require.Equal(t, NoCompression, info.CompressionHeaderID)

	contentID1, err1 := bm.WriteContent(ctx,
		indirectMetadataContent(),
		"x",
		NoCompression)
	require.NoError(t, err1)

	info1, err1 := bm.ContentInfo(ctx, contentID1)
	require.NoError(t, err1)
	require.Equal(t, NoCompression, info1.CompressionHeaderID)
}

func (s *contentManagerSuite) TestCompressionOfMetadata(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion: index.Version2,
	})

	//nolint:lll
	contentID, err := bm.WriteContent(ctx,
		dirMetadataContent(),
		"k",
		compression.HeaderZstdFastest)
	require.NoError(t, err)

	info, err := bm.ContentInfo(ctx, contentID)
	require.NoError(t, err)

	if bm.SupportsContentCompression() {
		require.Equal(t, compression.HeaderZstdFastest, info.CompressionHeaderID)
	} else {
		require.Equal(t, NoCompression, info.CompressionHeaderID)
	}

	contentID1, err1 := bm.WriteContent(ctx,
		indirectMetadataContent(),
		"x",
		compression.HeaderZstdFastest)
	require.NoError(t, err1)

	info1, err1 := bm.ContentInfo(ctx, contentID1)
	require.NoError(t, err1)

	if bm.SupportsContentCompression() {
		require.Equal(t, compression.HeaderZstdFastest, info1.CompressionHeaderID)
	} else {
		require.Equal(t, NoCompression, info1.CompressionHeaderID)
	}
}

func (s *contentManagerSuite) TestContentReadAliasing(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, faketime.Frozen(fakeTime))

	bm := s.newTestContentManagerWithCustomTime(t, st, faketime.Frozen(fakeTime))
	defer bm.CloseShared(ctx)

	contentData := []byte{100, 0, 0}
	id1 := writeContentAndVerify(ctx, t, bm, contentData)

	contentData2, err := bm.GetContent(ctx, id1)
	if err != nil {
		t.Fatalf("can't get content data: %v", err)
	}

	contentData2[0]++

	verifyContent(ctx, t, bm, id1, contentData)
	bm.Flush(ctx)
	verifyContent(ctx, t, bm, id1, contentData)
}

func (s *contentManagerSuite) TestVersionCompatibility(t *testing.T) {
	for writeVer := format.MinSupportedReadVersion; writeVer <= format.CurrentWriteVersion; writeVer++ {
		t.Run(fmt.Sprintf("version-%v", writeVer), func(t *testing.T) {
			s.verifyVersionCompat(t, writeVer)
		})
	}
}

func (s *contentManagerSuite) verifyVersionCompat(t *testing.T, writeVersion format.Version) {
	t.Helper()

	ctx := testlogging.Context(t)

	// create content manager that writes 'writeVersion' and reads all versions >= minSupportedReadVersion
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)

	mgr := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		formatVersion: writeVersion,
	})
	defer mgr.CloseShared(ctx)

	dataSet := map[ID][]byte{}

	for i := 0; i < 3000000; i = (i + 1) * 2 {
		data := make([]byte, i)
		cryptorand.Read(data)

		cid, err := mgr.WriteContent(ctx, gather.FromSlice(data), "", NoCompression)
		if err != nil {
			t.Fatalf("unable to write %v bytes: %v", len(data), err)
		}

		dataSet[cid] = data
	}
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	// delete random 3 items (map iteration order is random)
	cnt := 0

	for blobID := range dataSet {
		t.Logf("deleting %v", blobID)
		require.NoError(t, mgr.DeleteContent(ctx, blobID))
		delete(dataSet, blobID)
		cnt++

		if cnt >= 3 {
			break
		}
	}

	if err := mgr.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// create new manager that reads and writes using new version.
	mgr = s.newTestContentManager(t, st)
	defer mgr.CloseShared(ctx)

	// make sure we can read everything
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	if err := mgr.CompactIndexes(ctx, indexblob.CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Fatalf("unable to compact indexes: %v", err)
	}

	if err := mgr.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	// now open one more manager
	mgr = s.newTestContentManager(t, st)
	defer mgr.CloseShared(ctx)
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)
}

func (s *contentManagerSuite) TestReadsOwnWritesWithEventualConsistencyPersistentOwnWritesCache(t *testing.T) {
	data := blobtesting.DataMap{}
	timeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, nil, timeNow)
	cacheData := blobtesting.DataMap{}
	cacheKeyTime := map[blob.ID]time.Time{}
	cacheSt := blobtesting.NewMapStorage(cacheData, cacheKeyTime, timeNow)
	ecst := blobtesting.NewEventuallyConsistentStorage(
		logging.NewWrapper(st, testlogging.NewTestLogger(t), "[STORAGE] "),
		3*time.Second,
		timeNow)

	// disable own writes cache, will still be ok if store is strongly consistent
	s.verifyReadsOwnWrites(t, ownwrites.NewWrapper(ecst, cacheSt, cachedIndexBlobPrefixes, ownWritesCacheDuration), timeNow)
}

func (s *contentManagerSuite) TestReadsOwnWritesWithStrongConsistencyAndNoCaching(t *testing.T) {
	data := blobtesting.DataMap{}
	timeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, nil, timeNow)

	// if we used nullOwnWritesCache and eventual consistency, the test would fail
	// st = blobtesting.NewEventuallyConsistentStorage(logging.NewWrapper(st, t.Logf, "[STORAGE] "), 0.1)

	// disable own writes cache, will still be ok if store is strongly consistent
	s.verifyReadsOwnWrites(t, st, timeNow)
}

func (s *contentManagerSuite) verifyReadsOwnWrites(t *testing.T, st blob.Storage, timeNow func() time.Time) {
	t.Helper()

	ctx := testlogging.Context(t)

	tweaks := &contentManagerTestTweaks{
		ManagerOptions: ManagerOptions{
			TimeNow: timeNow,
		},
	}

	bm := s.newTestContentManagerWithTweaks(t, st, tweaks)

	ids := make([]ID, 100)
	for i := range len(ids) { //nolint:intrange
		ids[i] = writeContentAndVerify(ctx, t, bm, seededRandomData(i, maxPackCapacity/2))

		for j := range i {
			// verify all contents written so far
			verifyContent(ctx, t, bm, ids[j], seededRandomData(j, maxPackCapacity/2))
		}

		// every 10 contents, create new content manager
		if i%10 == 0 {
			t.Logf("------- flushing & reopening -----")
			require.NoError(t, bm.Flush(ctx))
			require.NoError(t, bm.CloseShared(ctx))
			bm = s.newTestContentManagerWithTweaks(t, st, tweaks)
		}
	}

	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))
	bm = s.newTestContentManagerWithTweaks(t, st, tweaks)

	for i := range len(ids) { //nolint:intrange
		verifyContent(ctx, t, bm, ids[i], seededRandomData(i, maxPackCapacity/2))
	}
}

func verifyContentManagerDataSet(ctx context.Context, t *testing.T, mgr *WriteManager, dataSet map[ID][]byte) {
	t.Helper()

	for contentID, originalPayload := range dataSet {
		v, err := mgr.GetContent(ctx, contentID)
		if err != nil {
			t.Fatalf("unable to read content %q: %v", contentID, err)
		}

		if !bytes.Equal(v, originalPayload) {
			t.Errorf("payload for %q does not match original: %v", v, originalPayload)
		}
	}
}

func (s *contentManagerSuite) TestCompression_Disabled(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion: index.Version1,
	})

	ctx := testlogging.Context(t)
	compressibleData := bytes.Repeat([]byte{1, 2, 3, 4}, 1000)

	// with index v1 the compression is disabled
	_, err := bm.WriteContent(ctx, gather.FromSlice(compressibleData), "", compression.ByName["pgzip"].HeaderID())
	require.Error(t, err)
}

func (s *contentManagerSuite) TestCompression_CompressibleData(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion: index.Version2,
	})

	ctx := testlogging.Context(t)
	compressibleData := bytes.Repeat([]byte{1, 2, 3, 4}, 1000)
	headerID := compression.ByName["gzip"].HeaderID()

	cid, err := bm.WriteContent(ctx, gather.FromSlice(compressibleData), "", headerID)
	require.NoError(t, err)

	ci, err := bm.ContentInfo(ctx, cid)
	require.NoError(t, err)

	// gzip-compressed length
	require.Equal(t, uint32(79), ci.PackedLength)
	require.Equal(t, uint32(len(compressibleData)), ci.OriginalLength)
	require.Equal(t, headerID, ci.CompressionHeaderID)

	verifyContent(ctx, t, bm, cid, compressibleData)

	require.NoError(t, bm.Flush(ctx))
	verifyContent(ctx, t, bm, cid, compressibleData)

	bm2 := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion: index.Version2,
	})
	verifyContent(ctx, t, bm2, cid, compressibleData)
}

func (s *contentManagerSuite) TestCompression_NonCompressibleData(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	bm := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion: index.Version2,
	})

	ctx := testlogging.Context(t)
	nonCompressibleData := make([]byte, 65000)
	headerID := compression.ByName["pgzip"].HeaderID()

	randRead(nonCompressibleData)

	cid, err := bm.WriteContent(ctx, gather.FromSlice(nonCompressibleData), "", headerID)
	require.NoError(t, err)

	verifyContent(ctx, t, bm, cid, nonCompressibleData)

	ci, err := bm.ContentInfo(ctx, cid)
	require.NoError(t, err)

	// verify compression did not occur
	require.Greater(t, ci.PackedLength, ci.OriginalLength)
	require.Equal(t, uint32(len(nonCompressibleData)), ci.OriginalLength)
	require.Equal(t, NoCompression, ci.CompressionHeaderID)

	require.NoError(t, bm.Flush(ctx))
	verifyContent(ctx, t, bm, cid, nonCompressibleData)

	bm2 := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion: index.Version2,
	})
	verifyContent(ctx, t, bm2, cid, nonCompressibleData)
}

func (s *contentManagerSuite) TestContentCachingByFormat(t *testing.T) {
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	cd := testutil.TempDirectory(t)

	// create two managers sharing cache directory
	co := CachingOptions{
		CacheDirectory:         cd,
		ContentCacheSizeBytes:  100e6,
		MetadataCacheSizeBytes: 100e6,
	}

	compressibleData := gather.FromSlice(bytes.Repeat([]byte{1, 2, 3, 4}, 10000))

	bm1 := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion:   index.Version2,
		CachingOptions: co,
	})

	bm2 := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		indexVersion:   index.Version2,
		CachingOptions: co,
	})

	ctx := testlogging.Context(t)

	id1, err := bm1.WriteContent(ctx, compressibleData, "", compression.ByName["pgzip"].HeaderID())
	require.NoError(t, err)

	id2, err := bm2.WriteContent(ctx, compressibleData, "", NoCompression)
	require.NoError(t, err)

	require.Equal(t, id1, id2)

	require.NoError(t, bm1.Flush(ctx))
	require.NoError(t, bm2.Flush(ctx))

	v1, err := bm1.GetContent(ctx, id1)
	require.NoError(t, err)

	v2, err := bm2.GetContent(ctx, id1)
	require.NoError(t, err)

	require.Equal(t, v1, v2)
}

func contentIDCacheKey(id ID) string {
	return cache.ContentIDCacheKey(id.String()) + ".0.1.0"
}

func (s *contentManagerSuite) TestPrefetchContent(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	st := blobtesting.NewMapStorage(data, nil, nil)
	cd := testutil.TempDirectory(t)
	bm := s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		CachingOptions: CachingOptions{
			CacheDirectory:         cd,
			ContentCacheSizeBytes:  100e6,
			MetadataCacheSizeBytes: 100e6,
		},
		maxPackSize: 20e6,
	})

	defer bm.CloseShared(ctx)
	bm.Flush(ctx)

	// write 6 x 6 MB content in 2 blobs.
	id1 := writeContentAndVerify(ctx, t, bm, bytes.Repeat([]byte{1, 2, 3, 4, 5, 6}, 1e6))
	id2 := writeContentAndVerify(ctx, t, bm, bytes.Repeat([]byte{2, 3, 4, 5, 6, 7}, 1e6))
	id3 := writeContentAndVerify(ctx, t, bm, bytes.Repeat([]byte{3, 4, 5, 6, 7, 8}, 1e6))
	require.NoError(t, bm.Flush(ctx))
	id4 := writeContentAndVerify(ctx, t, bm, bytes.Repeat([]byte{4, 5, 6, 7, 8, 9}, 1e6))
	id5 := writeContentAndVerify(ctx, t, bm, bytes.Repeat([]byte{5, 6, 7, 8, 9, 10}, 1e6))
	id6 := writeContentAndVerify(ctx, t, bm, bytes.Repeat([]byte{6, 7, 8, 9, 10, 11}, 1e6))
	require.NoError(t, bm.Flush(ctx))

	blob1 := getContentInfo(t, bm, id1).PackBlobID
	require.Equal(t, blob1, getContentInfo(t, bm, id2).PackBlobID)
	require.Equal(t, blob1, getContentInfo(t, bm, id3).PackBlobID)
	blob2 := getContentInfo(t, bm, id4).PackBlobID
	require.Equal(t, blob2, getContentInfo(t, bm, id5).PackBlobID)
	require.Equal(t, blob2, getContentInfo(t, bm, id6).PackBlobID)

	ccd := bm.contentCache
	ccm := bm.metadataCache

	hints := []string{
		"", "default", "contents", "blobs", "none",
	}

	cases := []struct {
		name              string
		input             []ID
		wantResult        []ID
		wantDataCacheKeys map[string][]string
	}{
		{
			name:       "MultipleBlobs",
			input:      []ID{id1, id2, id3, id4, id5, id6},
			wantResult: []ID{id1, id2, id3, id4, id5, id6},
			wantDataCacheKeys: map[string][]string{
				"":         {cache.BlobIDCacheKey(blob1), cache.BlobIDCacheKey(blob2)},
				"default":  {cache.BlobIDCacheKey(blob1), cache.BlobIDCacheKey(blob2)},
				"contents": {contentIDCacheKey(id1), contentIDCacheKey(id2), contentIDCacheKey(id3), contentIDCacheKey(id4), contentIDCacheKey(id5), contentIDCacheKey(id6)},
				"blobs":    {cache.BlobIDCacheKey(blob1), cache.BlobIDCacheKey(blob2)},
				"none":     {},
			},
		},
		{
			name:       "SingleContent",
			input:      []ID{id1},
			wantResult: []ID{id1},
			wantDataCacheKeys: map[string][]string{
				"":         {contentIDCacheKey(id1)},
				"default":  {contentIDCacheKey(id1)},
				"contents": {contentIDCacheKey(id1)},
				"blobs":    {cache.BlobIDCacheKey(blob1)},
				"none":     {},
			},
		},
		{
			name:       "TwoContentsFromSeparateBlobs",
			input:      []ID{id1, id4},
			wantResult: []ID{id1, id4},
			wantDataCacheKeys: map[string][]string{
				"":         {contentIDCacheKey(id1), contentIDCacheKey(id4)},
				"default":  {contentIDCacheKey(id1), contentIDCacheKey(id4)},
				"contents": {contentIDCacheKey(id1), contentIDCacheKey(id4)},
				"blobs":    {cache.BlobIDCacheKey(blob1), cache.BlobIDCacheKey(blob2)},
				"none":     {},
			},
		},
		{
			name:       "MixedContentsAndBlobs",
			input:      []ID{id1, id4, id5},
			wantResult: []ID{id1, id4, id5},
			wantDataCacheKeys: map[string][]string{
				"":         {contentIDCacheKey(id1), cache.BlobIDCacheKey(blob2)},
				"default":  {contentIDCacheKey(id1), cache.BlobIDCacheKey(blob2)},
				"contents": {contentIDCacheKey(id1), contentIDCacheKey(id4), contentIDCacheKey(id5)},
				"blobs":    {cache.BlobIDCacheKey(blob1), cache.BlobIDCacheKey(blob2)},
				"none":     {},
			},
		},
	}

	for _, hint := range hints {
		t.Run("hint:"+hint, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					wipeCache(t, ccd.CacheStorage())
					wipeCache(t, ccm.CacheStorage())

					require.Empty(t, allCacheKeys(t, ccd.CacheStorage()))
					require.Empty(t, allCacheKeys(t, ccm.CacheStorage()))

					require.Equal(t, tc.wantResult,
						bm.PrefetchContents(ctx, tc.input, hint))

					require.ElementsMatch(t, tc.wantDataCacheKeys[hint], allCacheKeys(t, ccd.CacheStorage()))

					for _, cid := range tc.wantResult {
						_, err := bm.GetContent(ctx, cid)
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

// TestContentPermissiveCacheLoading check that permissive reads read content as recorded.
func (s *contentManagerSuite) TestContentPermissiveCacheLoading(t *testing.T) {
	data := blobtesting.DataMap{}
	timeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, nil, timeNow)

	ctx := testlogging.Context(t)

	tweaks := &contentManagerTestTweaks{
		ManagerOptions: ManagerOptions{
			TimeNow: timeNow,
		},
	}

	bm := s.newTestContentManagerWithTweaks(t, st, tweaks)

	ids := make([]ID, 100)
	for i := range ids {
		ids[i] = writeContentAndVerify(ctx, t, bm, seededRandomData(i, maxPackCapacity/2))

		for j := range i {
			// verify all contents written so far
			verifyContent(ctx, t, bm, ids[j], seededRandomData(j, maxPackCapacity/2))
		}

		// every 10 contents, create new content manager
		if i%10 == 0 {
			t.Logf("------- flushing & reopening -----")
			require.NoError(t, bm.Flush(ctx))
			require.NoError(t, bm.CloseShared(ctx))
			bm = s.newTestContentManagerWithTweaks(t, st, tweaks)
		}
	}

	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))

	tweaks = &contentManagerTestTweaks{
		ManagerOptions: ManagerOptions{
			TimeNow:                timeNow,
			PermissiveCacheLoading: true,
		},
	}

	bm = s.newTestContentManagerWithTweaks(t, st, tweaks)

	for i := range ids {
		verifyContent(ctx, t, bm, ids[i], seededRandomData(i, maxPackCapacity/2))
	}
}

// TestContentIndexPermissiveReadsWithFault check that permissive reads read content as recorded.
func (s *contentManagerSuite) TestContentIndexPermissiveReadsWithFault(t *testing.T) {
	data := blobtesting.DataMap{}
	timeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, nil, timeNow)

	ctx := testlogging.Context(t)

	tweaks := &contentManagerTestTweaks{
		ManagerOptions: ManagerOptions{
			TimeNow: timeNow,
		},
	}

	bm := s.newTestContentManagerWithTweaks(t, st, tweaks)

	ids := make([]ID, 100)
	for i := range len(ids) { //nolint:intrange
		ids[i] = writeContentAndVerify(ctx, t, bm, seededRandomData(i, maxPackCapacity/2))

		for j := range i {
			// verify all contents written so far
			verifyContent(ctx, t, bm, ids[j], seededRandomData(j, maxPackCapacity/2))
		}

		// every 10 contents, create new content manager
		if i%10 == 0 {
			t.Logf("------- flushing & reopening -----")
			require.NoError(t, bm.Flush(ctx))
			require.NoError(t, bm.CloseShared(ctx))
			bm = s.newTestContentManagerWithTweaks(t, st, tweaks)
		}
	}

	require.NoError(t, format.WriteLegacyIndexPoisonBlob(ctx, st))

	require.NoError(t, bm.Flush(ctx))
	require.NoError(t, bm.CloseShared(ctx))

	tweaks = &contentManagerTestTweaks{
		ManagerOptions: ManagerOptions{
			TimeNow:                timeNow,
			PermissiveCacheLoading: true,
		},
	}

	bm = s.newTestContentManagerWithTweaks(t, st, tweaks)

	for i := range len(ids) { //nolint:intrange
		verifyContent(ctx, t, bm, ids[i], seededRandomData(i, maxPackCapacity/2))
	}
}

func wipeCache(t *testing.T, st cache.Storage) {
	t.Helper()

	ctx := testlogging.Context(t)

	require.NoError(t, st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return st.DeleteBlob(ctx, bm.BlobID)
	}))
}

func allCacheKeys(t *testing.T, st cache.Storage) []string {
	t.Helper()

	ctx := testlogging.Context(t)

	var entries []string

	require.NoError(t, st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		entries = append(entries, string(bm.BlobID))
		return nil
	}))

	return entries
}

func (s *contentManagerSuite) newTestContentManager(t *testing.T, st blob.Storage) *WriteManager {
	t.Helper()

	return s.newTestContentManagerWithTweaks(t, st, nil)
}

func (s *contentManagerSuite) newTestContentManagerWithCustomTime(t *testing.T, st blob.Storage, timeFunc func() time.Time) *WriteManager {
	t.Helper()

	return s.newTestContentManagerWithTweaks(t, st, &contentManagerTestTweaks{
		ManagerOptions: ManagerOptions{
			TimeNow: timeFunc,
		},
	})
}

type contentManagerTestTweaks struct {
	CachingOptions
	ManagerOptions

	indexVersion  int
	maxPackSize   int
	formatVersion format.Version
}

func (s *contentManagerSuite) newTestContentManagerWithTweaks(t *testing.T, st blob.Storage, tweaks *contentManagerTestTweaks) *WriteManager {
	t.Helper()

	if tweaks == nil {
		tweaks = &contentManagerTestTweaks{}
	}

	if tweaks.TimeNow == nil {
		tweaks.TimeNow = faketime.AutoAdvance(fakeTime, 1*time.Second)
	}

	mp := s.mutableParameters
	if tweaks.indexVersion != 0 {
		mp.IndexVersion = tweaks.indexVersion
	}

	mp.Version = 1

	if mps := tweaks.maxPackSize; mps != 0 {
		mp.MaxPackSize = mps
	}

	if tweaks.formatVersion != 0 {
		mp.Version = tweaks.formatVersion
	}

	ctx := testlogging.Context(t)
	fo := mustCreateFormatProvider(t, &format.ContentFormat{
		Hash:              "HMAC-SHA256",
		Encryption:        "AES256-GCM-HMAC-SHA256",
		HMACSecret:        hmacSecret,
		MutableParameters: mp,
	})

	bm, err := NewManagerForTesting(ctx, st, fo, &tweaks.CachingOptions, &tweaks.ManagerOptions)
	if err != nil {
		panic("can't create content manager: " + err.Error())
	}

	t.Cleanup(func() {
		bm.CloseShared(ctx)
	})

	bm.checkInvariantsOnUnlock = true

	return bm
}

func verifyContentNotFound(ctx context.Context, t *testing.T, bm *WriteManager, contentID ID) {
	t.Helper()

	b, err := bm.GetContent(ctx, contentID)
	if !errors.Is(err, ErrContentNotFound) {
		t.Fatalf("unexpected response from GetContent(%q), got %v,%v, expected %v", contentID, b, err, ErrContentNotFound)
	}
}

func verifyDeletedContentRead(ctx context.Context, t *testing.T, bm *WriteManager, contentID ID, b []byte) {
	t.Helper()
	verifyContent(ctx, t, bm, contentID, b)

	ci, err := bm.ContentInfo(ctx, contentID)
	if err != nil {
		t.Errorf("error getting content info %q: %v", contentID, err)
		return
	}

	if !ci.Deleted {
		t.Errorf("Expected content to be deleted, but it is not: %#v", ci)
	}
}

func verifyContent(ctx context.Context, t *testing.T, bm *WriteManager, contentID ID, b []byte) {
	t.Helper()

	b2, err := bm.GetContent(ctx, contentID)
	if err != nil {
		t.Errorf("unable to read content %q: %v", contentID, err)

		return
	}

	if got, want := b2, b; !bytes.Equal(got, want) {
		t.Errorf("content %q data mismatch: got %x (nil:%v), wanted %x (nil:%v)", contentID, got, got == nil, want, want == nil)
	}

	if _, err := bm.ContentInfo(ctx, contentID); err != nil {
		t.Errorf("error getting content info %q: %v", contentID, err)
	}
}

func writeContentAndVerify(ctx context.Context, t *testing.T, bm *WriteManager, b []byte) ID {
	t.Helper()

	contentID, err := bm.WriteContent(ctx, gather.FromSlice(b), "", NoCompression)
	if err != nil {
		t.Errorf("err: %v", err)

		return contentID
	}

	if got, want := contentID, hashValue(t, b); got != want {
		t.Errorf("invalid content ID for %x, got %v, want %v", b, got, want)
	}

	verifyContent(ctx, t, bm, contentID, b)

	return contentID
}

func flushWithRetries(ctx context.Context, t *testing.T, bm *WriteManager) int {
	t.Helper()

	var retryCount int

	err := bm.Flush(ctx)
	for i := 0; err != nil && i < maxRetries; i++ {
		t.Logf("flush failed %v, retrying", err)
		err = bm.Flush(ctx)
		retryCount++
	}

	if err != nil {
		t.Fatalf("err: %v", err)
	}

	return retryCount
}

func writeContentWithRetriesAndVerify(ctx context.Context, t *testing.T, bm *WriteManager, b []byte) (contentID ID, retryCount int) {
	t.Helper()

	t.Logf("*** starting writeContentWithRetriesAndVerify")

	contentID, err := bm.WriteContent(ctx, gather.FromSlice(b), "", NoCompression)
	for i := 0; err != nil && i < maxRetries; i++ {
		retryCount++

		t.Logf("*** try %v", retryCount)

		contentID, err = bm.WriteContent(ctx, gather.FromSlice(b), "", NoCompression)
	}

	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := contentID, hashValue(t, b); got != want {
		t.Errorf("invalid content ID for %x, got %v, want %v", b, got, want)
	}

	verifyContent(ctx, t, bm, contentID, b)
	t.Logf("*** finished after %v retries", retryCount)

	return contentID, retryCount
}

func seededRandomData(seed, length int) []byte {
	b := make([]byte, length)
	rnd := rand.New(rand.NewSource(int64(seed)))
	rnd.Read(b)

	return b
}

func hashValue(t *testing.T, b []byte) ID {
	t.Helper()

	h := hmac.New(sha256.New, hmacSecret)
	h.Write(b)

	id, err := IDFromHash("", h.Sum(nil))
	assert.NoError(t, err)

	return id
}

func dumpContentManagerData(t *testing.T, data blobtesting.DataMap) {
	t.Helper()
	t.Logf("***data - %v items", len(data))

	for k, v := range data {
		if k[0] == 'n' {
			t.Logf("index %v (%v bytes)", k, len(v))
		} else {
			t.Logf("non-index %v (%v bytes)\n", k, len(v))
		}
	}

	t.Logf("*** end of data")
}

func makeRandomHexID(t *testing.T, length int) index.ID {
	t.Helper()

	b := make([]byte, length/2)
	if _, err := randRead(b); err != nil {
		t.Fatal("Could not read random bytes", err)
	}

	id, err := IDFromHash("", b)
	require.NoError(t, err)

	return id
}

func deleteContent(ctx context.Context, t *testing.T, bm *WriteManager, c ID) {
	t.Helper()

	if err := bm.DeleteContent(ctx, c); err != nil {
		t.Fatalf("Unable to delete content %v: %v", c, err)
	}
}

func getContentInfo(t *testing.T, bm *WriteManager, c ID) Info {
	t.Helper()

	i, err := bm.ContentInfo(testlogging.Context(t), c)
	if err != nil {
		t.Fatalf("Unable to get content info for %q: %v", c, err)
	}

	return i
}

func verifyBlobCount(t *testing.T, data blobtesting.DataMap, want map[blob.ID]int) {
	t.Helper()

	got := map[blob.ID]int{}

	for k := range data {
		got[k[0:1]]++
	}

	if !cmp.Equal(got, want) {
		t.Fatalf("unexpected blob count %v, want %v", got, want)
	}
}

func withDeleted(i Info) Info {
	i.Deleted = true

	return i
}

var (
	// +checklocks:rMu
	r   = rand.New(rand.NewSource(rand.Int63()))
	rMu sync.Mutex
)

func randRead(b []byte) (n int, err error) {
	rMu.Lock()
	n, err = r.Read(b)
	rMu.Unlock()

	return
}

func dirMetadataContent() gather.Bytes {
	return gather.FromSlice([]byte(`{"stream":"kopia:directory","entries":[{"name":".chglog","type":"d","mode":"0755","mtime":"2022-03-22T22:45:22.159239913-07:00","uid":501,"gid":20,"obj":"k18c2fa7d9108a2bf0d9d5b8e7993c48d","summ":{"size":1897,"files":2,"symlinks":0,"dirs":1,"maxTime":"2022-03-22T22:45:22.159499411-07:00","numFailed":0}},{"name":".git","type":"d","mode":"0755","mtime":"2022-04-03T17:47:38.340226306-07:00","uid":501,"gid":20,"obj":"k0ad4214eb961aa78cf06611ec4563086","summ":{"size":88602907,"files":7336,"symlinks":0,"dirs":450,"maxTime":"2022-04-03T17:28:54.030135198-07:00","numFailed":0}},{"name":".github","type":"d","mode":"0755","mtime":"2022-03-22T22:45:22.160470238-07:00","uid":501,"gid":20,"obj":"k76bee329054d5574d89a4e87c3f24088","summ":{"size":20043,"files":13,"symlinks":0,"dirs":2,"maxTime":"2022-03-22T22:45:22.162580934-07:00","numFailed":0}},{"name":".logs","type":"d","mode":"0750","mtime":"2021-11-06T13:43:35.082115457-07:00","uid":501,"gid":20,"obj":"k1e7d5bda28d6b684bb180cac16775c1c","summ":{"size":382943352,"files":1823,"symlinks":0,"dirs":122,"maxTime":"2021-11-06T13:43:45.111270118-07:00","numFailed":0}},{"name":".release","type":"d","mode":"0755","mtime":"2021-04-16T06:26:47-07:00","uid":501,"gid":20,"obj":"k0eb539316600015bf2861e593f68e18d","summ":{"size":159711446,"files":19,"symlinks":0,"dirs":1,"maxTime":"2021-04-16T06:26:47-07:00","numFailed":0}},{"name":".screenshots","type":"d","mode":"0755","mtime":"2022-01-29T00:12:29.023594487-08:00","uid":501,"gid":20,"obj":"k97f6dbc82e84c97c955364d12ddc44bd","summ":{"size":6770746,"files":53,"symlinks":0,"dirs":7,"maxTime":"2022-03-19T18:59:51.559099257-07:00","numFailed":0}},{"name":"app","type":"d","mode":"0755","mtime":"2022-03-26T22:28:51.863826565-07:00","uid":501,"gid":20,"obj":"k656b41b8679c2537392b3997648cf43e","summ":{"size":565633611,"files":44812,"symlinks":0,"dirs":7576,"maxTime":"2022-03-26T22:28:51.863946606-07:00","numFailed":0}},{"name":"cli","type":"d","mode":"0755","mtime":"2022-04-03T12:24:52.84319224-07:00","uid":501,"gid":20,"obj":"k04ab4f2a1da96c47f62a51f119dba14d","summ":{"size":468233,"files":164,"symlinks":0,"dirs":1,"maxTime":"2022-04-03T12:24:52.843267824-07:00","numFailed":0}},{"name":"dist","type":"d","mode":"0755","mtime":"2022-03-19T22:46:00.12834831-07:00","uid":501,"gid":20,"obj":"k19fc65da8a47b7702bf6b501b7f3e1b5","summ":{"size":3420732994,"files":315,"symlinks":0,"dirs":321,"maxTime":"2022-03-27T12:10:08.019195221-07:00","numFailed":0}},{"name":"fs","type":"d","mode":"0755","mtime":"2022-03-22T22:45:22.194955195-07:00","uid":501,"gid":20,"obj":"k1f0be83e34826450e651f16ba63c5b9c","summ":{"size":80421,"files":21,"symlinks":0,"dirs":6,"maxTime":"2022-03-22T22:45:22.195085778-07:00","numFailed":0}},{"name":"icons","type":"d","mode":"0755","mtime":"2022-01-23T12:06:14.739575928-08:00","uid":501,"gid":20,"obj":"k9e76c283312bdc6e562f66c7d6526396","summ":{"size":361744,"files":13,"symlinks":0,"dirs":1,"maxTime":"2021-03-12T19:28:45-08:00","numFailed":0}},{"name":"internal","type":"d","mode":"0755","mtime":"2022-04-02T18:14:02.459772332-07:00","uid":501,"gid":20,"obj":"k181db968f69045159753f8d6f3f3454f","summ":{"size":778467,"files":198,"symlinks":0,"dirs":56,"maxTime":"2022-04-03T12:24:52.844331708-07:00","numFailed":0}},{"name":"node_modules","type":"d","mode":"0755","mtime":"2021-05-16T15:45:19-07:00","uid":501,"gid":20,"obj":"kf2b636c57a7cc412739d2c10ca7ab0a3","summ":{"size":5061213,"files":361,"symlinks":0,"dirs":69,"maxTime":"2021-05-16T15:45:19-07:00","numFailed":0}},{"name":"repo","type":"d","mode":"0755","mtime":"2022-04-03T12:24:52.844407167-07:00","uid":501,"gid":20,"obj":"kb839dcd04d94a1b568f7f5e8fc809fab","summ":{"size":992877,"files":193,"symlinks":0,"dirs":27,"maxTime":"2022-04-03T17:47:31.211316848-07:00","numFailed":0}},{"name":"site","type":"d","mode":"0755","mtime":"2022-03-22T22:45:22.250939688-07:00","uid":501,"gid":20,"obj":"k5d8ce70ca4337c17219502963f0fe6d3","summ":{"size":58225583,"files":11387,"symlinks":0,"dirs":557,"maxTime":"2022-03-22T22:45:22.258280685-07:00","numFailed":0}},{"name":"snapshot","type":"d","mode":"0755","mtime":"2022-03-22T22:45:22.265723348-07:00","uid":501,"gid":20,"obj":"k6201166bd99c8fe85d53d742e92c81a6","summ":{"size":316009,"files":66,"symlinks":0,"dirs":6,"maxTime":"2022-03-26T23:04:24.313115653-07:00","numFailed":0}},{"name":"tests","type":"d","mode":"0755","mtime":"2022-03-22T22:45:22.2749515-07:00","uid":501,"gid":20,"obj":"k1e20890089f6cbad3c6fe79cbae71e09","summ":{"size":657360,"files":183,"symlinks":0,"dirs":30,"maxTime":"2022-04-02T18:41:02.232496031-07:00","numFailed":0}},{"name":"tools","type":"d","mode":"0755","mtime":"2022-03-22T22:45:22.279094142-07:00","uid":501,"gid":20,"obj":"k6464e940fea5ef916ab86eafdb68b1cd","summ":{"size":889231805,"files":12412,"symlinks":0,"dirs":3405,"maxTime":"2022-03-22T22:45:22.279144141-07:00","numFailed":0}},{"name":".DS_Store","type":"f","mode":"0644","size":14340,"mtime":"2022-02-12T20:06:35.60110891-08:00","uid":501,"gid":20,"obj":"d9295958410ae3b73f68033274cd7a8f"},{"name":".codecov.yml","type":"f","mode":"0644","size":620,"mtime":"2022-03-22T22:45:22.159772743-07:00","uid":501,"gid":20,"obj":"6f81038ca8d7b81804f42031142731ed"},{"name":".gitattributes","type":"f","mode":"0644","size":340,"mtime":"2022-03-22T22:45:22.159870909-07:00","uid":501,"gid":20,"obj":"5608c2d289164627e8bdb468bbee2643"},{"name":".gitignore","type":"f","mode":"0644","size":321,"mtime":"2022-03-22T22:45:22.162843932-07:00","uid":501,"gid":20,"obj":"c43ce513c6371e0838fc553b77f5cdb2"},{"name":".golangci.yml","type":"f","mode":"0644","size":3071,"mtime":"2022-03-22T22:45:22.163100014-07:00","uid":501,"gid":20,"obj":"4289f49e43fba6800fa75462bd2ad43e"},{"name":".gometalinter.json","type":"f","mode":"0644","size":163,"mtime":"2019-05-09T22:33:06-07:00","uid":501,"gid":20,"obj":"fe4fc9d77cfb5f1b062414fdfd121713"},{"name":".goreleaser.yml","type":"f","mode":"0644","size":1736,"mtime":"2022-03-22T22:45:22.163354888-07:00","uid":501,"gid":20,"obj":"91093a462f4f72c619fb9f144702c1bf"},{"name":".linterr.txt","type":"f","mode":"0644","size":425,"mtime":"2021-11-08T22:14:29.315279172-08:00","uid":501,"gid":20,"obj":"f6c165387b84c7fb0ebc26fdc812775d"},{"name":".tmp.integration-tests.json","type":"f","mode":"0644","size":5306553,"mtime":"2022-03-27T12:10:55.035217892-07:00","uid":501,"gid":20,"obj":"Ixbc27b9a704275d05a6505e794ce63e66"},{"name":".tmp.provider-tests.json","type":"f","mode":"0644","size":617740,"mtime":"2022-02-15T21:30:28.579546866-08:00","uid":501,"gid":20,"obj":"e7f69fc0222763628d5b294faf37a6d7"},{"name":".tmp.unit-tests.json","type":"f","mode":"0644","size":200525943,"mtime":"2022-04-03T10:08:51.453180251-07:00","uid":501,"gid":20,"obj":"Ixf5da1bbcdbc267fa123d93aaf90cbd75"},{"name":".wwhrd.yml","type":"f","mode":"0644","size":244,"mtime":"2022-03-22T22:45:22.163564803-07:00","uid":501,"gid":20,"obj":"cea0cac6d19d59dcf2818b08521f46b8"},{"name":"BUILD.md","type":"f","mode":"0644","size":4873,"mtime":"2022-03-22T22:45:22.163818593-07:00","uid":501,"gid":20,"obj":"bcd47eca7b520b3ea88e4799cc0c9fea"},{"name":"CODE_OF_CONDUCT.md","type":"f","mode":"0644","size":5226,"mtime":"2021-03-12T19:28:45-08:00","uid":501,"gid":20,"obj":"270e55b022ec0c7588b2dbb501791b3e"},{"name":"GOVERNANCE.md","type":"f","mode":"0644","size":12477,"mtime":"2020-03-15T23:40:35-07:00","uid":501,"gid":20,"obj":"96674fad8fcf2bdfb96b0583917bb617"},{"name":"LICENSE","type":"f","mode":"0644","size":10763,"mtime":"2019-05-27T15:50:18-07:00","uid":501,"gid":20,"obj":"e751b8a146e1dd5494564e9a8c26dd6a"},{"name":"Makefile","type":"f","mode":"0644","size":17602,"mtime":"2022-03-22T22:45:22.1639718-07:00","uid":501,"gid":20,"obj":"aa9cc80d567e94087ea9be8fef718c1a"},{"name":"README.md","type":"f","mode":"0644","size":3874,"mtime":"2022-03-22T22:45:22.164109925-07:00","uid":501,"gid":20,"obj":"d227c763b9cf476426da5d99e9fff694"},{"name":"a.log","type":"f","mode":"0644","size":3776,"mtime":"2022-03-08T19:19:40.196874627-08:00","uid":501,"gid":20,"obj":"6337190196e804297f92a17805600be7"},{"name":"build_architecture.svg","type":"f","mode":"0644","size":143884,"mtime":"2021-03-12T19:28:45-08:00","uid":501,"gid":20,"obj":"72c0aef8c43498b056236b2d46d7e44a"},{"name":"coverage.txt","type":"f","mode":"0644","size":194996,"mtime":"2022-03-26T07:09:37.533649628-07:00","uid":501,"gid":20,"obj":"fdf1a20cea21d4daf053b99711735d0e"},{"name":"go.mod","type":"f","mode":"0644","size":5447,"mtime":"2022-03-27T09:40:59.78753556-07:00","uid":501,"gid":20,"obj":"71eefc767aeea467b1d1f7ff0ee5c21b"},{"name":"go.sum","type":"f","mode":"0644","size":114899,"mtime":"2022-03-27T09:40:59.788485485-07:00","uid":501,"gid":20,"obj":"2e801e525d9e58208dff3c25bd30f296"},{"name":"main.go","type":"f","mode":"0644","size":2057,"mtime":"2022-03-22T22:45:22.22380977-07:00","uid":501,"gid":20,"obj":"73411f7e340e5cddc43faaa1d1fe5743"}],"summary":{"size":5787582078,"files":79395,"symlinks":0,"dirs":12639,"maxTime":"2022-04-03T17:47:38.340226306-07:00","numFailed":0}}`)) //nolint:lll
}

func indirectMetadataContent() gather.Bytes {
	return gather.FromSlice([]byte(`{"stream":"kopia:indirect","entries":[{"l":7616808,"o":"a6d555a7070f7e6c1e0c9cf90e8a6cc7"},{"s":7616808,"l":8388608,"o":"7ba10912378095851cff7da5f8083fc0"},{"s":16005416,"l":2642326,"o":"de41b93c1c1ba1f030d32e2cefffa0e9"},{"s":18647742,"l":2556388,"o":"25f391d185c3101006a45553efb67742"},{"s":21204130,"l":3156843,"o":"3b281271f7c0e17f533fe5edc0f79b31"},{"s":24360973,"l":8388608,"o":"4fb9395a4790fb0b6c5f0b91f102e9ab"},{"s":32749581,"l":8388608,"o":"bf0cfa2796354f0c74ee725af7a6824b"},{"s":41138189,"l":5788370,"o":"ecb6672792bfb433886b6e57d055ecd7"},{"s":46926559,"l":3828331,"o":"ac49ad086654c624f1e86a3d46ebdf04"},{"s":50754890,"l":6544699,"o":"951b34fddcc2cc679b23b074dabc7e4e"},{"s":57299589,"l":2523488,"o":"47965162d4ebc46b25a965854d4921d3"},{"s":59823077,"l":3510947,"o":"83d6c1f3ab9695075b93eeab6cc0761c"},{"s":63334024,"l":3239328,"o":"a8aa9f5ed5357520f0c0b04cb65293ec"},{"s":66573352,"l":8388608,"o":"9ca2f0ff2e50219759b4c07971ea4e84"},{"s":74961960,"l":3737528,"o":"5eaddb02c217c1d455078c858ae3ff96"},{"s":78699488,"l":2382189,"o":"513adbee65ed3f13fc6a6a27c1b683d1"},{"s":81081677,"l":3145876,"o":"a5968eb3ad727f4a6b263541a7847c7e"},{"s":84227553,"l":4302790,"o":"58929275a937192f01b1af8526c25cad"},{"s":88530343,"l":3795820,"o":"d2adf1e91029b37450ef988ff88bd861"},{"s":92326163,"l":8388608,"o":"9a14d257b93a9011a8d133ee3cd0c5bc"},{"s":100714771,"l":3885115,"o":"3ce2122c512d00744ab065ef8d782fe6"},{"s":104599886,"l":2109875,"o":"501a69a59ee5f3dd1b2c8add2fdc5cf8"},{"s":106709761,"l":6656155,"o":"6ba38db7fb389339b41dde4e8097e4ab"},{"s":113365916,"l":3789867,"o":"7b594f73ab9e3ad736aede2d1964e4e9"},{"s":117155783,"l":4156979,"o":"7215d07ec33b442aee52bd50234bf03d"},{"s":121312762,"l":4089475,"o":"d1ef2d9e330b11eec9365fefdc5434eb"},{"s":125402237,"l":8388608,"o":"38969b3114caf31a3501b34109063c25"},{"s":133790845,"l":8388608,"o":"cb1cf30e75d0fbbe058db1b8394e6e03"},{"s":142179453,"l":3645601,"o":"975e2cdb9ccbf36e3012a715c2a596de"},{"s":145825054,"l":2546129,"o":"2e2b6b2e98fbfcdc1855f5f36d8c2fb7"},{"s":148371183,"l":2830247,"o":"535dffb5b1df8f5f6f8d9787d961f81e"},{"s":151201430,"l":7158506,"o":"f953277da0845c6fe42d0e115219e6d6"},{"s":158359936,"l":2705426,"o":"83130d0e230071c5a94d38e3e94cf326"},{"s":161065362,"l":7085401,"o":"6b75fb5f5ab5728282bb043cf6d96cd3"},{"s":168150763,"l":5357359,"o":"431c63e39c20b879e517861acf12091f"},{"s":173508122,"l":5426372,"o":"0f329762d79c6948261dcde8fa26b3b8"},{"s":178934494,"l":6322719,"o":"dc8c1d8c09c0ce783e932ae2279c3db5"},{"s":185257213,"l":8388608,"o":"b5cb9fc5464c30f7bacfda0e5381ae91"},{"s":193645821,"l":3711229,"o":"494f1e15cfea3ab09523a391df0fbebc"},{"s":197357050,"l":6853193,"o":"a0c91d2654cfd2b4ca34542bb4b5d926"},{"s":204210243,"l":2645205,"o":"1cfcab6023b83e32c284c8eb1310f34c"},{"s":206855448,"l":5775640,"o":"84baf20ed2f84ba09f317028a366532d"},{"s":212631088,"l":2698898,"o":"7a6746a097f4506956f5e8d56eee6873"},{"s":215329986,"l":3444532,"o":"b11be0bf84341a0cbcd46ca14b6fed6d"},{"s":218774518,"l":5042437,"o":"3bc63ab43d9b7c19b42d51508f449b8b"},{"s":223816955,"l":4407710,"o":"f4cb0dcb6ad0d1d17c52ef7f5654d7b9"},{"s":228224665,"l":3288967,"o":"0a9254bb39e95e9a93c30b10f03e2f2a"},{"s":231513632,"l":6818881,"o":"fa22cfbe6caebb301dc4eae4d8d13a9b"},{"s":238332513,"l":4224104,"o":"29a1316a5157b0a3359b2760cbd0895c"},{"s":242556617,"l":4427385,"o":"0efe5d26d520d4ab114fcddb8d1a1931"},{"s":246984002,"l":3625567,"o":"8e6b4a4e1acc6100a271a9100518ff77"},{"s":250609569,"l":5412145,"o":"d3988a71021a70c0ff69eb0d80dca0c8"},{"s":256021714,"l":8388608,"o":"0b5c245c16e8fb845358f75a2f984585"},{"s":264410322,"l":8388608,"o":"70d149b1ec039dc716ae3b524f1ef0f8"},{"s":272798930,"l":5295221,"o":"a081eb5227d37e8d00343c450bc12117"},{"s":278094151,"l":3320852,"o":"7394c656b6278445ad39189dec6896f8"},{"s":281415003,"l":4569639,"o":"9e80f48dc5aa9378d1c4206d17dc3116"},{"s":285984642,"l":3227911,"o":"bd486cf43401ef78ae1199c6c18cb424"},{"s":289212553,"l":4408113,"o":"f73c366a16745ca5fe823c4074e026b4"},{"s":293620666,"l":5806890,"o":"fba0357b2a79b20ba3b942c0f22d545b"},{"s":299427556,"l":8388608,"o":"6e805d1757fa230794ab8445d377c832"},{"s":307816164,"l":5026069,"o":"88e75d7ba957fbe150e5c49a501540a6"},{"s":312842233,"l":8388608,"o":"17e65917f54e4e0b454c93eb08a8c342"},{"s":321230841,"l":2416356,"o":"e65ce9c2efe34ea01d015c737abc060a"},{"s":323647197,"l":2129020,"o":"b89cb59bb69a32e865d9afbf454d080e"},{"s":325776217,"l":6264283,"o":"6a80f62763f33d2946844ef3a8755517"},{"s":332040500,"l":7998871,"o":"59bce9d16094aef2e07f98098039bd91"},{"s":340039371,"l":3760705,"o":"53b191c6dfb41134b3430343438bf4ae"},{"s":343800076,"l":8388608,"o":"8d8945a17b9a819d03f414a337c2e47d"},{"s":352188684,"l":4370796,"o":"d216de504cdbc7a598c067e49f26c69b"},{"s":356559480,"l":8388608,"o":"e6f7e4cce390627c7030a9774ed885b1"},{"s":364948088,"l":4673010,"o":"32865f3c19fcf194e7fde39ef2e6aa28"},{"s":369621098,"l":8388608,"o":"26139bd21b4581d4b97be682f13005c9"},{"s":378009706,"l":3305716,"o":"5fe7a3d8d80e4dc367021ece1130b203"},{"s":381315422,"l":8388608,"o":"00a029bd5a9a63cde2ba9d25ebea11f7"},{"s":389704030,"l":8388608,"o":"67c10d19567b60a4193ab73bfc77ae99"},{"s":398092638,"l":5533146,"o":"045bcfb7416579d060c10f82946eae1b"},{"s":403625784,"l":8388608,"o":"72cda208c56f5c7bbfc99b65889bfc80"},{"s":412014392,"l":3760763,"o":"6cb3f59c8823c049e222b58c8c155d1e"},{"s":415775155,"l":3552185,"o":"d71b9f954d280b03f54c90db61168fc2"},{"s":419327340,"l":8388608,"o":"66df8620bdd389b079cc0334c4fb0f04"},{"s":427715948,"l":3653017,"o":"796520ac43adcaec6117760fc2699b78"},{"s":431368965,"l":2935638,"o":"01fea89a93279431a0a7f5188ceefed1"},{"s":434304603,"l":2820579,"o":"c9b3a1868f00f55d90cf02aa3c877b05"},{"s":437125182,"l":8388608,"o":"d77d35d2ead1595aedc25a65069e8d88"},{"s":445513790,"l":7407288,"o":"2297b4fb6ca3959a7fb0220e358a9778"},{"s":452921078,"l":7891558,"o":"a2cd30afaafcb844405eb6f048323bbc"},{"s":460812636,"l":3191130,"o":"ba6b77fc177cf223b1d50bf330ebf8ce"},{"s":464003766,"l":7565430,"o":"ea273aa565f457e94beca5e1d20ec068"},{"s":471569196,"l":3419794,"o":"eedd34de4ae36993f04f75ebc3c9a165"},{"s":474988990,"l":3460292,"o":"2a851cea2d84ca661b3eebf72cf0de55"},{"s":478449282,"l":8032042,"o":"b402c287796218ddf5d3fff2e70eb2c7"},{"s":486481324,"l":6320993,"o":"6fec73dd933316685cc3de99b6c0be66"},{"s":492802317,"l":2960958,"o":"386bfb6cf878efc2881aacfef8c8c22d"},{"s":495763275,"l":4043015,"o":"eaa10fc56a85813899e15e87ba458c90"},{"s":499806290,"l":2220895,"o":"94e8e439c139f120d514d248cb1d37b7"},{"s":502027185,"l":2318042,"o":"ccd572f48087ee0dce5af0d1823279cf"},{"s":504345227,"l":3396237,"o":"c1080ad8f97a38eaa3754023d0ff616c"},{"s":507741464,"l":3761426,"o":"abd1cc7cb7332535f1672e1fd0b48967"},{"s":511502890,"l":3313883,"o":"030705ce77d9eb02d3e91fa7a2f5ee16"},{"s":514816773,"l":4643444,"o":"56c1e4ca5e2bc64d1744e6645f16fec2"},{"s":519460217,"l":4877742,"o":"83f88295b8539647b759aab1e7588a5f"},{"s":524337959,"l":2731173,"o":"d3fc29a18a49f05f5320592f043b3898"},{"s":527069132,"l":4388381,"o":"0d206d6e7240945ccc2900814604e55d"},{"s":531457513,"l":4198048,"o":"87c54dab1f99b6b44e4193e4e7cbf6b1"},{"s":535655561,"l":8300001,"o":"d1d2be80c5e1942e8742481df1acc022"},{"s":543955562,"l":2103894,"o":"213b91aeb37f106cd97e29d23306d492"},{"s":546059456,"l":3464612,"o":"0cec1bb256cb1f37b65339ee4df7eaa4"},{"s":549524068,"l":6456134,"o":"5b21a9c34210b23e0d1711ffb467e694"},{"s":555980202,"l":4180529,"o":"f77ebea3c198350bb255bdfc0fdf6a36"},{"s":560160731,"l":8388608,"o":"9893ebd1ef51a280861b1168f9e838af"},{"s":568549339,"l":3672532,"o":"40f3c47adb19bec122d9647e1b7986ad"},{"s":572221871,"l":4686009,"o":"ffa5697af8444e22bdf05cd7f7b4e211"},{"s":576907880,"l":8388608,"o":"3ee328d1cb9f862a928198ecb28ae7b6"},{"s":585296488,"l":3117981,"o":"cbdb5e9e2390e031571567ffaf81ba08"},{"s":588414469,"l":8388608,"o":"9212fbcd5b2c5b09475f387b7a54d25c"},{"s":596803077,"l":8388608,"o":"5f06b16231dd3038abe59ddf17789e89"},{"s":605191685,"l":5345215,"o":"b22a5da98d6a3909d5e171998abfdc13"},{"s":610536900,"l":8388608,"o":"93db1f2b3e5272fffc3d644ec00f1463"},{"s":618925508,"l":7526887,"o":"d2b612202fa49f2fd059f76057183fd9"},{"s":626452395,"l":6650357,"o":"5863fec408b1aa89ccf1c77a1e29061e"},{"s":633102752,"l":8388608,"o":"4295a43614c097a8a4f72bb1f8d3cf3a"},{"s":641491360,"l":2281701,"o":"13e34075d962bcfdb89dcbd5b766aee6"},{"s":643773061,"l":4494718,"o":"b6cc56aba7510b753a3dae94428b62ff"},{"s":648267779,"l":6378335,"o":"9a8a3c3fe94e205523e40b2ed7eb902b"},{"s":654646114,"l":8388608,"o":"2636ee206c0a3c3b099b3f9f2e36eec6"},{"s":663034722,"l":8388608,"o":"e6323f8542eb34ad197099074b08ff55"},{"s":671423330,"l":8388608,"o":"66f6a6485ac08085328996b28ced7452"},{"s":679811938,"l":7119415,"o":"170721a5d1a9728df40deedcb5bde060"},{"s":686931353,"l":2960051,"o":"f52f94fbaf8d101e633c545b5b0cdf24"},{"s":689891404,"l":4571243,"o":"cc47bfaa5b6d54dd863bc714cc607f82"},{"s":694462647,"l":7146332,"o":"331722c804700da0c4fa4c43d04aa56a"},{"s":701608979,"l":5152399,"o":"f4668768e6c15d00b8d02c1d20faecca"},{"s":706761378,"l":8388608,"o":"593addeedf8da213289758348e05567c"},{"s":715149986,"l":8388608,"o":"388715dd8b32f2088572c7703302b596"},{"s":723538594,"l":4120402,"o":"0947e4864bd26230e26406f117b18d4c"},{"s":727658996,"l":8103740,"o":"ae3062a4e74d4a407b944c895dfe1f95"},{"s":735762736,"l":4037896,"o":"2fb24ad127cbe65fc704cfdd15d3e4c2"},{"s":739800632,"l":6316726,"o":"6f21491d81b688d5efbe0ff22e35e05b"},{"s":746117358,"l":3007919,"o":"eaa42376365bad6707f4c11c204d65eb"},{"s":749125277,"l":5262875,"o":"321847ff2d9c62f7f2c6db3914327756"},{"s":754388152,"l":4462123,"o":"c565fa31ef90fc2c196d9cde44095597"},{"s":758850275,"l":5294675,"o":"c6baec6e22d1c604a04d887aeed1fd82"},{"s":764144950,"l":2912994,"o":"1327ac0489a8e76c1fbebe5b561ca6b4"},{"s":767057944,"l":2962702,"o":"97fc763b782a57f9fd542f4ab7657a85"},{"s":770020646,"l":8388608,"o":"1ca3bce935b5d306be767a9c89cf0026"},{"s":778409254,"l":365274,"o":"484b0358354388fdd16d9ea2cfe9260d"}]}`)) //nolint:lll
}
