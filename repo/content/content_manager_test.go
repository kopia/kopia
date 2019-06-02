package content

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	logging "github.com/op/go-logging"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
)

const (
	maxPackSize = 2000
)

var fakeTime = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
var hmacSecret = []byte{1, 2, 3}

func init() {
	logging.SetLevel(logging.DEBUG, "")
}

func TestContentManagerEmptyFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	bm.Flush(ctx)
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentZeroBytes1(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	contentID := writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
	dumpContentManagerData(t, data)
	bm = newTestContentManager(data, keyTime, nil)
	verifyContent(ctx, t, bm, contentID, []byte{})
}

func TestContentZeroBytes2(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	writeContentAndVerify(ctx, t, bm, seededRandomData(10, 10))
	writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
		dumpContentManagerData(t, data)
	}
}

func TestContentManagerSmallContentWrites(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		writeContentAndVerify(ctx, t, bm, seededRandomData(i, 10))
	}
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerDedupesPendingContents(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		writeContentAndVerify(ctx, t, bm, seededRandomData(0, 999))
	}
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerDedupesPendingAndUncommittedContents(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	// no writes here, all data fits in a single pack.
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, 10))
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	// no writes here
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, 950))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, 10))
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
	bm.Flush(ctx)

	// this flushes the pack content + index blob
	if got, want := len(data), 2; got != want {
		dumpContentManagerData(t, data)
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerEmpty(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	noSuchContentID := ID(hashValue([]byte("foo")))

	b, err := bm.GetContent(ctx, noSuchContentID)
	if err != ErrContentNotFound {
		t.Errorf("unexpected error when getting non-existent content: %v, %v", b, err)
	}

	bi, err := bm.ContentInfo(ctx, noSuchContentID)
	if err != ErrContentNotFound {
		t.Errorf("unexpected error when getting non-existent content info: %v, %v", bi, err)
	}

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func verifyActiveIndexBlobCount(ctx context.Context, t *testing.T, bm *Manager, expected int) {
	t.Helper()

	blks, err := bm.IndexBlobs(ctx)
	if err != nil {
		t.Errorf("error listing active index blobs: %v", err)
		return
	}

	if got, want := len(blks), expected; got != want {
		t.Errorf("unexpected number of active index blobs %v, expected %v (%v)", got, want, blks)
	}
}
func TestContentManagerInternalFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		rand.Read(b)
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 1 data content written, but no index yet.
	if got, want := len(data), 1; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	// do it again - should be 2 contents + 1000 bytes pending.
	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		rand.Read(b)
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 2 data contents written, but no index yet.
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	// third content gets written, followed by index.
	if got, want := len(data), 4; got != want {
		dumpContentManagerData(t, data)
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerWriteMultiple(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	timeFunc := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
	bm := newTestContentManager(data, keyTime, timeFunc)

	var contentIDs []ID

	for i := 0; i < 5000; i++ {
		//t.Logf("i=%v", i)
		b := seededRandomData(i, i%113)
		blkID, err := bm.WriteContent(ctx, b, "")
		if err != nil {
			t.Errorf("err: %v", err)
		}

		contentIDs = append(contentIDs, blkID)

		if i%17 == 0 {
			//t.Logf("flushing %v", i)
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}
			//dumpContentManagerData(t, data)
		}

		if i%41 == 0 {
			//t.Logf("opening new manager: %v", i)
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}
			//t.Logf("data content count: %v", len(data))
			//dumpContentManagerData(t, data)
			bm = newTestContentManager(data, keyTime, timeFunc)
		}

		pos := rand.Intn(len(contentIDs))
		if _, err := bm.GetContent(ctx, contentIDs[pos]); err != nil {
			dumpContentManagerData(t, data)
			t.Fatalf("can't read content %q: %v", contentIDs[pos], err)
			continue
		}
	}
}

// This is regression test for a bug where we would corrupt data when encryption
// was done in place and clobbered pending data in memory.
func TestContentManagerFailedToWritePack(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)
	faulty := &blobtesting.FaultyStorage{
		Base: st,
	}
	st = faulty

	bm, err := newManagerWithOptions(context.Background(), st, FormattingOptions{
		Version:     1,
		Hash:        "HMAC-SHA256-128",
		Encryption:  "AES-256-CTR",
		MaxPackSize: maxPackSize,
		HMACSecret:  []byte("foo"),
		MasterKey:   []byte("0123456789abcdef0123456789abcdef"),
	}, CachingOptions{}, fakeTimeNowFrozen(fakeTime), nil)
	if err != nil {
		t.Fatalf("can't create bm: %v", err)
	}
	logging.SetLevel(logging.DEBUG, "faulty-storage")

	faulty.Faults = map[string][]*blobtesting.Fault{
		"PutContent": {
			{Err: errors.New("booboo")},
		},
	}

	b1, err := bm.WriteContent(ctx, seededRandomData(1, 10), "")
	if err != nil {
		t.Fatalf("can't create content: %v", err)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Logf("expected flush error: %v", err)
	}

	verifyContent(ctx, t, bm, b1, seededRandomData(1, 10))
}

func TestContentManagerConcurrency(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	preexistingContent := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)

	dumpContentManagerData(t, data)
	bm1 := newTestContentManager(data, keyTime, nil)
	bm2 := newTestContentManager(data, keyTime, nil)
	bm3 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(1), 1*time.Second))

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
	bm4 := newTestContentManager(data, keyTime, nil)
	verifyContent(ctx, t, bm4, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm4, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm4, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm4, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm4, bm3content, seededRandomData(33, 100))

	if got, want := getIndexCount(data), 4; got != want {
		t.Errorf("unexpected index count before compaction: %v, wanted %v", got, want)
	}

	if err := bm4.CompactIndexes(ctx, CompactOptions{
		MinSmallBlobs: 1,
		MaxSmallBlobs: 1,
	}); err != nil {
		t.Errorf("compaction error: %v", err)
	}
	if got, want := getIndexCount(data), 1; got != want {
		t.Errorf("unexpected index count after compaction: %v, wanted %v", got, want)
	}

	// new content manager at this point can see all data.
	bm5 := newTestContentManager(data, keyTime, nil)
	verifyContent(ctx, t, bm5, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm5, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm5, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm5, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm5, bm3content, seededRandomData(33, 100))
	if err := bm5.CompactIndexes(ctx, CompactOptions{
		MinSmallBlobs: 1,
		MaxSmallBlobs: 1,
	}); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func TestDeleteContent(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)
	content2 := writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))
	if err := bm.DeleteContent(content1); err != nil {
		t.Errorf("unable to delete content: %v", content1)
	}
	if err := bm.DeleteContent(content2); err != nil {
		t.Errorf("unable to delete content: %v", content1)
	}
	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
	bm.Flush(ctx)
	log.Debugf("-----------")
	bm = newTestContentManager(data, keyTime, nil)
	//dumpContentManagerData(t, data)
	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
}

func TestRewriteNonDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> RewriteContent() <action2> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			t.Run(fmt.Sprintf("case-%v-%v", action1, action2), func(t *testing.T) {
				ctx := context.Background()
				data := blobtesting.DataMap{}
				keyTime := map[blob.ID]time.Time{}
				fakeNow := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
				bm := newTestContentManager(data, keyTime, fakeNow)

				applyStep := func(action int) {
					switch action {
					case 0:
						t.Logf("flushing and reopening")
						bm.Flush(ctx)
						bm = newTestContentManager(data, keyTime, fakeNow)
					case 1:
						t.Logf("flushing")
						bm.Flush(ctx)
					case 2:
						t.Logf("doing nothing")
					}
				}

				content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
				applyStep(action1)
				assertNoError(t, bm.RewriteContent(ctx, content1))
				applyStep(action2)
				verifyContent(ctx, t, bm, content1, seededRandomData(10, 100))
				dumpContentManagerData(t, data)
			})
		}
	}
}

func TestDisableFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	bm.DisableIndexFlush()
	bm.DisableIndexFlush()
	for i := 0; i < 500; i++ {
		writeContentAndVerify(ctx, t, bm, seededRandomData(i, 100))
	}
	bm.Flush(ctx) // flush will not have effect
	bm.EnableIndexFlush()
	bm.Flush(ctx) // flush will not have effect
	bm.EnableIndexFlush()

	verifyActiveIndexBlobCount(ctx, t, bm, 0)
	bm.EnableIndexFlush()
	verifyActiveIndexBlobCount(ctx, t, bm, 0)
	bm.Flush(ctx) // flush will happen now
	verifyActiveIndexBlobCount(ctx, t, bm, 1)
}

func TestRewriteDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> Delete() <action2> RewriteContent() <action3> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			for action3 := 0; action3 < stepBehaviors; action3++ {
				t.Run(fmt.Sprintf("case-%v-%v-%v", action1, action2, action3), func(t *testing.T) {
					ctx := context.Background()
					data := blobtesting.DataMap{}
					keyTime := map[blob.ID]time.Time{}
					fakeNow := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
					bm := newTestContentManager(data, keyTime, fakeNow)

					applyStep := func(action int) {
						switch action {
						case 0:
							t.Logf("flushing and reopening")
							bm.Flush(ctx)
							bm = newTestContentManager(data, keyTime, fakeNow)
						case 1:
							t.Logf("flushing")
							bm.Flush(ctx)
						case 2:
							t.Logf("doing nothing")
						}
					}

					content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
					applyStep(action1)
					assertNoError(t, bm.DeleteContent(content1))
					applyStep(action2)
					if got, want := bm.RewriteContent(ctx, content1), ErrContentNotFound; got != want && got != nil {
						t.Errorf("unexpected error %v, wanted %v", got, want)
					}
					applyStep(action3)
					verifyContentNotFound(ctx, t, bm, content1)
					dumpContentManagerData(t, data)
				})
			}
		}
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	ctx := context.Background()
	// simulate race between delete/recreate and delete
	// delete happens at t0+10, recreate at t0+20 and second delete time is parameterized.
	// depending on it, the second delete results will be visible.
	cases := []struct {
		desc         string
		deletionTime time.Time
		isVisible    bool
	}{
		{"deleted before delete and-recreate", fakeTime.Add(5 * time.Second), true},
		//{"deleted after delete and recreate", fakeTime.Add(25 * time.Second), false},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// write a content
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}
			bm := newTestContentManager(data, keyTime, fakeTimeNowFrozen(fakeTime))
			content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
			bm.Flush(ctx)

			// delete but at given timestamp but don't commit yet.
			bm0 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(tc.deletionTime, 1*time.Second))
			assertNoError(t, bm0.DeleteContent(content1))

			// delete it at t0+10
			bm1 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(10*time.Second), 1*time.Second))
			verifyContent(ctx, t, bm1, content1, seededRandomData(10, 100))
			assertNoError(t, bm1.DeleteContent(content1))
			bm1.Flush(ctx)

			// recreate at t0+20
			bm2 := newTestContentManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(20*time.Second), 1*time.Second))
			content2 := writeContentAndVerify(ctx, t, bm2, seededRandomData(10, 100))
			bm2.Flush(ctx)

			// commit deletion from bm0 (t0+5)
			bm0.Flush(ctx)

			//dumpContentManagerData(t, data)

			if content1 != content2 {
				t.Errorf("got invalid content %v, expected %v", content2, content1)
			}

			bm3 := newTestContentManager(data, keyTime, nil)
			dumpContentManagerData(t, data)
			if tc.isVisible {
				verifyContent(ctx, t, bm3, content1, seededRandomData(10, 100))
			} else {
				verifyContentNotFound(ctx, t, bm3, content1)
			}
		})
	}
}

func TestFindUnreferencedBlobs(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	contentID := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	if err := bm.DeleteContent(contentID); err != nil {
		t.Errorf("error deleting content: %v", contentID)
	}
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// content still present in first pack
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)

	assertNoError(t, bm.RewriteContent(ctx, contentID))
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 1)
	assertNoError(t, bm.RewriteContent(ctx, contentID))
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 2)
}

func TestFindUnreferencedBlobs2(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, nil)
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	contentID := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))
	dumpContents(t, bm, "after writing")
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	dumpContents(t, bm, "after flush")
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	if err := bm.DeleteContent(contentID); err != nil {
		t.Errorf("error deleting content: %v", contentID)
	}
	dumpContents(t, bm, "after delete")
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	dumpContents(t, bm, "after flush")
	// content present in first pack, original pack is still referenced
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
}

func dumpContents(t *testing.T, bm *Manager, caption string) {
	t.Helper()
	infos, err := bm.ListContentInfos("", true)
	if err != nil {
		t.Errorf("error listing contents: %v", err)
		return
	}

	log.Infof("**** dumping %v contents %v", len(infos), caption)
	for i, bi := range infos {
		log.Debugf(" bi[%v]=%#v", i, bi)
	}
	log.Infof("finished dumping %v contents", len(infos))
}

func verifyUnreferencedStorageFilesCount(ctx context.Context, t *testing.T, bm *Manager, want int) {
	t.Helper()
	unref, err := bm.FindUnreferencedBlobs(ctx)
	if err != nil {
		t.Errorf("error in FindUnreferencedBlobs: %v", err)
	}

	log.Infof("got %v expecting %v", unref, want)
	if got := len(unref); got != want {
		t.Errorf("invalid number of unreferenced contents: %v, wanted %v", got, want)
	}
}

func TestContentWriteAliasing(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, fakeTimeNowFrozen(fakeTime))

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

func TestContentReadAliasing(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(data, keyTime, fakeTimeNowFrozen(fakeTime))

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

func TestVersionCompatibility(t *testing.T) {
	for writeVer := minSupportedReadVersion; writeVer <= currentWriteVersion; writeVer++ {
		t.Run(fmt.Sprintf("version-%v", writeVer), func(t *testing.T) {
			verifyVersionCompat(t, writeVer)
		})
	}
}

func verifyVersionCompat(t *testing.T, writeVersion int) {
	ctx := context.Background()

	// create content manager that writes 'writeVersion' and reads all versions >= minSupportedReadVersion
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	mgr := newTestContentManager(data, keyTime, nil)
	mgr.writeFormatVersion = int32(writeVersion)

	dataSet := map[ID][]byte{}

	for i := 0; i < 3000000; i = (i + 1) * 2 {
		data := make([]byte, i)
		rand.Read(data)

		cid, err := mgr.WriteContent(ctx, data, "")
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
		assertNoError(t, mgr.DeleteContent(blobID))
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
	mgr = newTestContentManager(data, keyTime, nil)

	// make sure we can read everything
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	if err := mgr.CompactIndexes(ctx, CompactOptions{
		MinSmallBlobs: 1,
		MaxSmallBlobs: 1,
	}); err != nil {
		t.Fatalf("unable to compact indexes: %v", err)
	}
	if err := mgr.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	// now open one more manager
	mgr = newTestContentManager(data, keyTime, nil)
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)
}

func verifyContentManagerDataSet(ctx context.Context, t *testing.T, mgr *Manager, dataSet map[ID][]byte) {
	for contentID, originalPayload := range dataSet {
		v, err := mgr.GetContent(ctx, contentID)
		if err != nil {
			t.Errorf("unable to read content %q: %v", contentID, err)
			continue
		}

		if !reflect.DeepEqual(v, originalPayload) {
			t.Errorf("payload for %q does not match original: %v", v, originalPayload)
		}
	}
}

func newTestContentManager(data blobtesting.DataMap, keyTime map[blob.ID]time.Time, timeFunc func() time.Time) *Manager {
	//st = logging.NewWrapper(st)
	if timeFunc == nil {
		timeFunc = fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
	}
	st := blobtesting.NewMapStorage(data, keyTime, timeFunc)
	bm, err := newManagerWithOptions(context.Background(), st, FormattingOptions{
		Hash:        "HMAC-SHA256",
		Encryption:  "NONE",
		HMACSecret:  hmacSecret,
		MaxPackSize: maxPackSize,
		Version:     1,
	}, CachingOptions{}, timeFunc, nil)
	if err != nil {
		panic("can't create content manager: " + err.Error())
	}
	bm.checkInvariantsOnUnlock = true
	return bm
}

func getIndexCount(d blobtesting.DataMap) int {
	var cnt int

	for blobID := range d {
		if strings.HasPrefix(string(blobID), newIndexBlobPrefix) {
			cnt++
		}
	}

	return cnt
}

func fakeTimeNowFrozen(t time.Time) func() time.Time {
	return fakeTimeNowWithAutoAdvance(t, 0)
}

func fakeTimeNowWithAutoAdvance(t time.Time, dt time.Duration) func() time.Time {
	var mu sync.Mutex
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		ret := t
		t = t.Add(dt)
		return ret
	}
}

func verifyContentNotFound(ctx context.Context, t *testing.T, bm *Manager, contentID ID) {
	t.Helper()

	b, err := bm.GetContent(ctx, contentID)
	if err != ErrContentNotFound {
		t.Errorf("unexpected response from GetContent(%q), got %v,%v, expected %v", contentID, b, err, ErrContentNotFound)
	}
}

func verifyContent(ctx context.Context, t *testing.T, bm *Manager, contentID ID, b []byte) {
	t.Helper()

	b2, err := bm.GetContent(ctx, contentID)
	if err != nil {
		t.Errorf("unable to read content %q: %v", contentID, err)
		return
	}

	if got, want := b2, b; !reflect.DeepEqual(got, want) {
		t.Errorf("content %q data mismatch: got %x (nil:%v), wanted %x (nil:%v)", contentID, got, got == nil, want, want == nil)
	}

	bi, err := bm.ContentInfo(ctx, contentID)
	if err != nil {
		t.Errorf("error getting content info %q: %v", contentID, err)
	}

	if got, want := bi.Length, uint32(len(b)); got != want {
		t.Errorf("invalid content size for %q: %v, wanted %v", contentID, got, want)
	}

}
func writeContentAndVerify(ctx context.Context, t *testing.T, bm *Manager, b []byte) ID {
	t.Helper()

	contentID, err := bm.WriteContent(ctx, b, "")
	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := contentID, ID(hashValue(b)); got != want {
		t.Errorf("invalid content ID for %x, got %v, want %v", b, got, want)
	}

	verifyContent(ctx, t, bm, contentID, b)

	return contentID
}

func seededRandomData(seed int, length int) []byte {
	b := make([]byte, length)
	rnd := rand.New(rand.NewSource(int64(seed)))
	rnd.Read(b)
	return b
}

func hashValue(b []byte) string {
	h := hmac.New(sha256.New, hmacSecret)
	h.Write(b) //nolint:errcheck
	return hex.EncodeToString(h.Sum(nil))
}

func dumpContentManagerData(t *testing.T, data blobtesting.DataMap) {
	t.Helper()
	for k, v := range data {
		if k[0] == 'n' {
			ndx, err := openPackIndex(bytes.NewReader(v))
			if err == nil {
				t.Logf("index %v (%v bytes)", k, len(v))
				assertNoError(t, ndx.Iterate("", func(i Info) error {
					t.Logf("  %+v\n", i)
					return nil
				}))

			}
		} else {
			t.Logf("data %v (%v bytes)\n", k, len(v))
		}
	}
}
