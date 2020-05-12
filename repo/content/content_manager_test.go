package content

import (
	"bytes"
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/logging"
)

const (
	maxPackSize     = 2000
	maxPackCapacity = maxPackSize - defaultMaxPreambleLength
	maxRetries      = 100

	encryptionOverhead = 12 + 16
)

var fakeTime = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
var hmacSecret = []byte{1, 2, 3}

func TestContentManagerEmptyFlush(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)
	bm.Flush(ctx)

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentZeroBytes1(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)
	contentID := writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	dumpContentManagerData(ctx, t, data)
	bm = newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)

	verifyContent(ctx, t, bm, contentID, []byte{})
}

func TestContentZeroBytes2(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)

	writeContentAndVerify(ctx, t, bm, seededRandomData(10, 10))
	writeContentAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)

	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
		dumpContentManagerData(ctx, t, data)
	}
}

func TestContentManagerSmallContentWrites(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)

	itemCount := maxPackCapacity / (10 + encryptionOverhead)
	for i := 0; i < itemCount; i++ {
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
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)

	for i := 0; i < 100; i++ {
		writeContentAndVerify(ctx, t, bm, seededRandomData(0, maxPackCapacity/2))
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
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)

	// compute content size so that 3 contents will fit in a pack without overflowing
	contentSize := maxPackCapacity/3 - encryptionOverhead - 1

	// no writes here, all data fits in a single pack.
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, contentSize))

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	// no writes here
	writeContentAndVerify(ctx, t, bm, seededRandomData(0, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(1, contentSize))
	writeContentAndVerify(ctx, t, bm, seededRandomData(2, contentSize))

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	// this flushes the pack content + index blob
	if got, want := len(data), 2; got != want {
		dumpContentManagerData(ctx, t, data)
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerEmpty(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)

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

	blks, err := bm.IndexBlobs(ctx, false)
	if err != nil {
		t.Errorf("error listing active index blobs: %v", err)
		return
	}

	if got, want := len(blks), expected; got != want {
		t.Errorf("unexpected number of active index blobs %v, expected %v (%v)", got, want, blks)
	}
}

func TestContentManagerInternalFlush(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	defer bm.Close(ctx)

	itemsToOverflow := (maxPackCapacity)/(25+encryptionOverhead) + 2
	for i := 0; i < itemsToOverflow; i++ {
		b := make([]byte, 25)
		cryptorand.Read(b) //nolint:errcheck
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 1 data content written, but no index yet.
	if got, want := len(data), 1; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	// do it again - should be 2 blobs + some bytes pending.
	for i := 0; i < itemsToOverflow; i++ {
		b := make([]byte, 25)
		cryptorand.Read(b) //nolint:errcheck
		writeContentAndVerify(ctx, t, bm, b)
	}

	// 2 data contents written, but no index yet.
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	// third content gets written, followed by index.
	if got, want := len(data), 4; got != want {
		dumpContentManagerData(ctx, t, data)
		t.Errorf("unexpected number of contents: %v, wanted %v", got, want)
	}
}

func TestContentManagerWriteMultiple(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	timeFunc := faketime.AutoAdvance(fakeTime, 1*time.Second)

	bm := newTestContentManager(t, data, keyTime, timeFunc)
	defer bm.Close(ctx)

	var contentIDs []ID

	for i := 0; i < 5000; i++ {
		b := seededRandomData(i, i%113)

		blkID, err := bm.WriteContent(ctx, b, "")
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

			bm = newTestContentManager(t, data, keyTime, timeFunc)
			defer bm.Close(ctx)
		}

		pos := rand.Intn(len(contentIDs))
		if _, err := bm.GetContent(ctx, contentIDs[pos]); err != nil {
			dumpContentManagerData(ctx, t, data)
			t.Fatalf("can't read content %q: %v", contentIDs[pos], err)

			continue
		}
	}
}

// This is regression test for a bug where we would corrupt data when encryption
// was done in place and clobbered pending data in memory.
func TestContentManagerFailedToWritePack(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)
	faulty := &blobtesting.FaultyStorage{
		Base: st,
	}
	st = faulty

	bm, err := newManagerWithOptions(testlogging.Context(t), st, &FormattingOptions{
		Version:     1,
		Hash:        "HMAC-SHA256-128",
		Encryption:  "AES256-GCM-HMAC-SHA256",
		MaxPackSize: maxPackSize,
		HMACSecret:  []byte("foo"),
		MasterKey:   []byte("0123456789abcdef0123456789abcdef"),
	}, nil, faketime.Frozen(fakeTime), nil)
	if err != nil {
		t.Fatalf("can't create bm: %v", err)
	}

	defer bm.Close(ctx)

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
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)

	preexistingContent := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)

	dumpContentManagerData(ctx, t, data)

	bm1 := newTestContentManager(t, data, keyTime, nil)
	defer bm1.Close(ctx)

	bm2 := newTestContentManager(t, data, keyTime, nil)
	defer bm2.Close(ctx)

	bm3 := newTestContentManager(t, data, keyTime, faketime.AutoAdvance(fakeTime.Add(1), 1*time.Second))
	defer bm3.Close(ctx)

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
	bm4 := newTestContentManager(t, data, keyTime, nil)
	defer bm4.Close(ctx)

	verifyContent(ctx, t, bm4, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm4, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm4, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm4, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm4, bm3content, seededRandomData(33, 100))

	validateIndexCount(t, data, 4, 0)

	if err := bm4.CompactIndexes(ctx, CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("compaction error: %v", err)
	}

	validateIndexCount(t, data, 5, 1)

	// new content manager at this point can see all data.
	bm5 := newTestContentManager(t, data, keyTime, nil)
	defer bm5.Close(ctx)

	verifyContent(ctx, t, bm5, preexistingContent, seededRandomData(10, 100))
	verifyContent(ctx, t, bm5, sharedContent, seededRandomData(20, 100))
	verifyContent(ctx, t, bm5, bm1content, seededRandomData(31, 100))
	verifyContent(ctx, t, bm5, bm2content, seededRandomData(32, 100))
	verifyContent(ctx, t, bm5, bm3content, seededRandomData(33, 100))

	if err := bm5.CompactIndexes(ctx, CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func validateIndexCount(t *testing.T, data map[blob.ID][]byte, wantIndexCount, wantCompactionLogCount int) {
	t.Helper()

	var indexCnt, compactionLogCnt int

	for blobID := range data {
		if strings.HasPrefix(string(blobID), indexBlobPrefix) {
			indexCnt++
		}

		if strings.HasPrefix(string(blobID), compactionLogBlobPrefix) {
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

func TestDeleteContent(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)

	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("error flushing: %v", err)
	}

	dumpContents(ctx, t, bm, "after first flush")

	content2 := writeContentAndVerify(ctx, t, bm, seededRandomData(11, 100))

	log(ctx).Infof("xxx deleting.")

	if err := bm.DeleteContent(ctx, content1); err != nil {
		t.Fatalf("unable to delete content %v: %v", content1, err)
	}

	log(ctx).Infof("yyy deleting.")

	if err := bm.DeleteContent(ctx, content2); err != nil {
		t.Fatalf("unable to delete content %v: %v", content2, err)
	}

	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
	log(ctx).Infof("flushing")
	bm.Flush(ctx)
	log(ctx).Infof("flushed")
	log(ctx).Debugf("-----------")

	bm = newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)
	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
}

// nolint:gocyclo
func TestUndeleteContentSimple(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

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
			cid:     ID(makeRandomHexString(t, len(content3))), // non-existing
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

		// ignore different timestamps, pack id and pack offset
		got.TimestampSeconds = want.TimestampSeconds
		got.PackBlobID = want.PackBlobID
		got.PackOffset = want.PackOffset

		if got != want {
			t.Errorf("content info does not match.\nwant: %#v\ngot:  %#v", want, got)
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
		got.TimestampSeconds = tc.want.TimestampSeconds
		got.PackBlobID = tc.want.PackBlobID
		got.PackOffset = tc.want.PackOffset

		if got != tc.want {
			t.Errorf("content info does not match.\nwant: %#v\ngot:  %#v", tc.want, got)
		}
	}
}

// nolint:gocyclo
func TestUndeleteContent(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

	content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(20, 10))
	content2 := writeContentAndVerify(ctx, t, bm, seededRandomData(21, 10))
	content3 := writeContentAndVerify(ctx, t, bm, seededRandomData(31, 10))

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("error flushing: %v", err)
	}

	dumpContents(ctx, t, bm, "after first flush")

	log(ctx).Infof("deleting content 1: %s", content1)

	if err := bm.DeleteContent(ctx, content1); err != nil {
		t.Fatalf("unable to delete content %v: %v", content1, err)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Fatalf("error flushing: %v", err)
	}

	log(ctx).Infof("deleting content 2: %s", content2)

	if err := bm.DeleteContent(ctx, content2); err != nil {
		t.Fatalf("unable to delete content %v: %v", content2, err)
	}

	content4 := writeContentAndVerify(ctx, t, bm, seededRandomData(41, 10))
	content5 := writeContentAndVerify(ctx, t, bm, seededRandomData(51, 10))

	log(ctx).Infof("deleting content 4: %s", content4)

	if err := bm.DeleteContent(ctx, content4); err != nil {
		t.Fatalf("unable to delete content %v: %v", content4, err)
	}

	verifyContentNotFound(ctx, t, bm, content1)
	verifyContentNotFound(ctx, t, bm, content2)
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

	log(ctx).Infof("flushing ...")
	bm.Flush(ctx)
	log(ctx).Infof("... flushed")

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

	bm = newTestContentManager(t, data, keyTime, nil)
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

func TestDeleteAfterUndelete(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestContentManager(t, data, keyTime, nil)

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
	c2Want.Deleted = true

	// delete content1 before flushing
	deleteContentAfterUndeleteAndCheck(ctx, t, bm, content1, c1Want)

	// now delete c2 after having flushed
	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	deleteContentAfterUndeleteAndCheck(ctx, t, bm, content2, c2Want)
}

func deleteContentAfterUndeleteAndCheck(ctx context.Context, t *testing.T, bm *Manager, id ID, want Info) { // nolint:gocritic
	t.Helper()
	deleteContent(ctx, t, bm, id)

	got := getContentInfo(t, bm, id)
	if !got.Deleted {
		t.Errorf("Expected content %q to be deleted, got: %#v", id, got)
	}

	// ignore timestamp
	got.TimestampSeconds = want.TimestampSeconds

	if want != got {
		t.Errorf("Content %q info does not match\nwant: %#v\ngot:  %#v", id, want, got)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Fatal("error while flushing:", err)
	}

	// check c1 again
	got = getContentInfo(t, bm, id)
	if !got.Deleted {
		t.Error("Expected content to be deleted, got: ", got)
	}

	// ignore timestamp
	got.TimestampSeconds = want.TimestampSeconds

	if !reflect.DeepEqual(want, got) {
		t.Errorf("Content info does not match\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestParallelWrites(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	// set up fake storage that is slow at PutBlob causing writes to be piling up
	fs := &blobtesting.FaultyStorage{
		Base: st,
		Faults: map[string][]*blobtesting.Fault{
			"PutBlob": {
				{
					Repeat: 1000000000,
					Sleep:  1 * time.Second,
				},
			},
		},
	}

	var workersWG sync.WaitGroup

	var workerLock sync.RWMutex

	bm := newTestContentManagerWithStorage(t, fs, nil)
	defer bm.Close(ctx)

	numWorkers := 8
	closeWorkers := make(chan bool)

	// workerLock allows workers to append to their own list of IDs (when R-locked) in parallel.
	// W-lock allows flusher to capture the state without any worker being able to modify it.
	workerWritten := make([][]ID, numWorkers)

	// start numWorkers, each writing random block and recording it
	for workerID := 0; workerID < numWorkers; workerID++ {
		workerID := workerID

		workersWG.Add(1)

		go func() {
			defer workersWG.Done()

			for {
				select {
				case <-closeWorkers:
					return
				case <-time.After(1 * time.Nanosecond):
					id := writeContentAndVerify(ctx, t, bm, seededRandomData(rand.Int(), 100)) //nolint:gosec

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
				log(ctx).Infof("closing flusher goroutine")
				return
			case <-time.After(2 * time.Second):
				log(ctx).Infof("about to flush")

				// capture snapshot of all content IDs while holding a writer lock
				allWritten := map[ID]bool{}

				workerLock.Lock()

				for _, ww := range workerWritten {
					for _, id := range ww {
						allWritten[id] = true
					}
				}

				workerLock.Unlock()

				log(ctx).Infof("captured %v contents", len(allWritten))

				if err := bm.Flush(ctx); err != nil {
					t.Errorf("flush error: %v", err)
				}

				// open new content manager and verify all contents are visible there.
				verifyAllDataPresent(ctx, t, data, allWritten)
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

func TestFlushResumesWriters(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	st := blobtesting.NewMapStorage(data, keyTime, nil)

	// set up fake storage that is slow at PutBlob causing writes to be piling up
	fs := &blobtesting.FaultyStorage{
		Base: st,
		Faults: map[string][]*blobtesting.Fault{
			"PutBlob": {
				{
					Repeat: 1000000000,
					Sleep:  3 * time.Second,
				},
			},
		},
	}

	bm := newTestContentManagerWithStorage(t, fs, nil)
	defer bm.Close(ctx)
	first := writeContentAndVerify(ctx, t, bm, []byte{1, 2, 3})

	var second ID

	var writeWG sync.WaitGroup

	writeWG.Add(1)

	go func() {
		defer writeWG.Done()

		// start a write while flush is ongoing, the write will block on the condition variable
		time.Sleep(1 * time.Second)
		log(ctx).Infof("write started")

		second = writeContentAndVerify(ctx, t, bm, []byte{3, 4, 5})

		log(ctx).Infof("write finished")
	}()

	// flush will take 5 seconds, 1 second into that we will start a write
	bm.Flush(ctx)

	// wait for write to complete, if this times out, Flush() is not waking up writers
	writeWG.Wait()

	verifyAllDataPresent(ctx, t, data, map[ID]bool{
		first: true,
	})

	// flush again, this will include buffer
	bm.Flush(ctx)

	verifyAllDataPresent(ctx, t, data, map[ID]bool{
		first:  true,
		second: true,
	})
}

func verifyAllDataPresent(ctx context.Context, t *testing.T, data map[blob.ID][]byte, contentIDs map[ID]bool) {
	bm := newTestContentManager(t, data, nil, nil)
	defer bm.Close(ctx)
	_ = bm.IterateContents(ctx, IterateOptions{}, func(ci Info) error {
		delete(contentIDs, ci.ID)
		return nil
	})

	if len(contentIDs) != 0 {
		t.Errorf("some blocks not written: %v", contentIDs)
	}
}

func TestHandleWriteErrors(t *testing.T) {
	ctx := testlogging.Context(t)

	// genFaults(S0,F0,S1,F1,...,) generates a list of faults
	// where success is returned Sn times followed by failure returned Fn times
	genFaults := func(counts ...int) []*blobtesting.Fault {
		var result []*blobtesting.Fault

		for i, cnt := range counts {
			if i%2 == 0 {
				result = append(result, &blobtesting.Fault{
					Repeat: cnt - 1,
				})
			} else {
				result = append(result, &blobtesting.Fault{
					Repeat: cnt - 1,
					Err:    errors.Errorf("some write error"),
				})
			}
		}

		return result
	}

	// simulate a stream of PutBlob failures, write some contents followed by flush
	// count how many times we retried writes/flushes
	// also, verify that all the data is durable
	cases := []struct {
		faults               []*blobtesting.Fault // failures to similuate
		numContents          int                  // how many contents to write
		contentSize          int                  // size of each content
		expectedFlushRetries int
		expectedWriteRetries int
	}{
		{faults: genFaults(0, 10, 10, 10, 10, 10, 10, 10, 10, 10), numContents: 5, contentSize: maxPackSize, expectedWriteRetries: 10, expectedFlushRetries: 0},
		{faults: genFaults(1, 2), numContents: 1, contentSize: maxPackSize, expectedWriteRetries: 0, expectedFlushRetries: 2},
		{faults: genFaults(1, 2), numContents: 10, contentSize: maxPackSize, expectedWriteRetries: 2, expectedFlushRetries: 0},
		// 2 failures, 2 successes (pack blobs), 1 failure (flush), 1 success (flush)
		{faults: genFaults(0, 2, 2, 1, 1, 1, 1), numContents: 2, contentSize: maxPackSize, expectedWriteRetries: 2, expectedFlushRetries: 1},
		{faults: genFaults(0, 2, 2, 1, 1, 1, 1), numContents: 4, contentSize: maxPackSize / 2, expectedWriteRetries: 2, expectedFlushRetries: 1},
	}

	for n, tc := range cases {
		tc := tc

		t.Run(fmt.Sprintf("case-%v", n), func(t *testing.T) {
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}
			st := blobtesting.NewMapStorage(data, keyTime, nil)

			// set up fake storage that is slow at PutBlob causing writes to be piling up
			fs := &blobtesting.FaultyStorage{
				Base: st,
				Faults: map[string][]*blobtesting.Fault{
					"PutBlob": tc.faults,
				},
			}

			bm := newTestContentManagerWithStorage(t, fs, nil)
			defer bm.Close(ctx)

			writeRetries := 0
			var cids []ID
			for i := 0; i < tc.numContents; i++ {
				cid, retries := writeContentWithRetriesAndVerify(ctx, t, bm, seededRandomData(i, tc.contentSize))
				writeRetries += retries
				cids = append(cids, cid)
			}
			if got, want := flushWithRetries(ctx, t, bm), tc.expectedFlushRetries; got != want {
				t.Errorf("invalid # of flush retries %v, wanted %v", got, want)
			}
			if got, want := writeRetries, tc.expectedWriteRetries; got != want {
				t.Errorf("invalid # of write retries %v, wanted %v", got, want)
			}

			bm2 := newTestContentManagerWithStorage(t, st, nil)
			defer bm2.Close(ctx)

			for i, cid := range cids {
				verifyContent(ctx, t, bm2, cid, seededRandomData(i, tc.contentSize))
			}
		})
	}
}

func TestRewriteNonDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> RewriteContent() <action2> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			action1 := action1
			action2 := action2

			t.Run(fmt.Sprintf("case-%v-%v", action1, action2), func(t *testing.T) {
				ctx := testlogging.Context(t)
				data := blobtesting.DataMap{}
				keyTime := map[blob.ID]time.Time{}
				fakeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
				bm := newTestContentManager(t, data, keyTime, fakeNow)
				defer bm.Close(ctx)

				applyStep := func(action int) {
					switch action {
					case 0:
						t.Logf("flushing and reopening")
						bm.Flush(ctx)
						bm = newTestContentManager(t, data, keyTime, fakeNow)
						defer bm.Close(ctx)
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
				dumpContentManagerData(ctx, t, data)
			})
		}
	}
}

func TestDisableFlush(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)

	bm.DisableIndexFlush(ctx)
	bm.DisableIndexFlush(ctx)

	for i := 0; i < 500; i++ {
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

func TestRewriteDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteContent() <action1> Delete() <action2> RewriteContent() <action3> GetContent()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			for action3 := 0; action3 < stepBehaviors; action3++ {
				action1 := action1
				action2 := action2
				action3 := action3
				t.Run(fmt.Sprintf("case-%v-%v-%v", action1, action2, action3), func(t *testing.T) {
					ctx := testlogging.Context(t)
					data := blobtesting.DataMap{}
					keyTime := map[blob.ID]time.Time{}
					fakeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
					bm := newTestContentManager(t, data, keyTime, fakeNow)
					defer bm.Close(ctx)

					applyStep := func(action int) {
						switch action {
						case 0:
							t.Logf("flushing and reopening")
							bm.Flush(ctx)
							bm = newTestContentManager(t, data, keyTime, fakeNow)
							defer bm.Close(ctx)
						case 1:
							t.Logf("flushing")
							bm.Flush(ctx)
						case 2:
							t.Logf("doing nothing")
						}
					}

					content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
					applyStep(action1)
					assertNoError(t, bm.DeleteContent(ctx, content1))
					applyStep(action2)
					if got, want := bm.RewriteContent(ctx, content1), ErrContentNotFound; got != want && got != nil {
						t.Errorf("unexpected error %v, wanted %v", got, want)
					}
					applyStep(action3)
					verifyContentNotFound(ctx, t, bm, content1)
					dumpContentManagerData(ctx, t, data)
				})
			}
		}
	}
}

func TestDeleteAndRecreate(t *testing.T) {
	ctx := testlogging.Context(t)
	// simulate race between delete/recreate and delete
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
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			// write a content
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}

			bm := newTestContentManager(t, data, keyTime, faketime.Frozen(fakeTime))
			defer bm.Close(ctx)

			content1 := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))
			bm.Flush(ctx)

			// delete but at given timestamp but don't commit yet.
			bm0 := newTestContentManager(t, data, keyTime, faketime.AutoAdvance(tc.deletionTime, 1*time.Second))
			defer bm0.Close(ctx)

			assertNoError(t, bm0.DeleteContent(ctx, content1))

			// delete it at t0+10
			bm1 := newTestContentManager(t, data, keyTime, faketime.AutoAdvance(fakeTime.Add(10*time.Second), 1*time.Second))
			defer bm1.Close(ctx)

			verifyContent(ctx, t, bm1, content1, seededRandomData(10, 100))
			assertNoError(t, bm1.DeleteContent(ctx, content1))
			bm1.Flush(ctx)

			// recreate at t0+20
			bm2 := newTestContentManager(t, data, keyTime, faketime.AutoAdvance(fakeTime.Add(20*time.Second), 1*time.Second))
			defer bm2.Close(ctx)

			content2 := writeContentAndVerify(ctx, t, bm2, seededRandomData(10, 100))
			bm2.Flush(ctx)

			// commit deletion from bm0 (t0+5)
			bm0.Flush(ctx)

			if content1 != content2 {
				t.Errorf("got invalid content %v, expected %v", content2, content1)
			}

			bm3 := newTestContentManager(t, data, keyTime, nil)
			defer bm3.Close(ctx)

			dumpContentManagerData(ctx, t, data)
			if tc.isVisible {
				verifyContent(ctx, t, bm3, content1, seededRandomData(10, 100))
			} else {
				verifyContentNotFound(ctx, t, bm3, content1)
			}
		})
	}
}

// nolint:funlen
func TestIterateContents(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)

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
			fail: someError,
			want: map[ID]bool{},
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
				Range: PrefixRange(contentID1),
			},
			want: map[ID]bool{contentID1: true},
		},
		{
			desc: "prefix, include deleted",
			options: IterateOptions{
				Range:          PrefixRange(contentID2),
				IncludeDeleted: true,
			},
			want: map[ID]bool{
				contentID2: true,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			var mu sync.Mutex
			got := map[ID]bool{}

			err := bm.IterateContents(ctx, tc.options, func(ci Info) error {
				if tc.fail != nil {
					return tc.fail
				}

				mu.Lock()
				got[ci.ID] = true
				mu.Unlock()
				return nil
			})

			if tc.fail != err {
				t.Errorf("error iterating: %v", err)
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("invalid content IDs got: %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFindUnreferencedBlobs(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)

	verifyUnreferencedBlobsCount(ctx, t, bm, 0)
	contentID := writeContentAndVerify(ctx, t, bm, seededRandomData(10, 100))

	log(ctx).Infof("flushing")

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(ctx, t, bm, "after flush #1")
	dumpContentManagerData(ctx, t, data)
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	if err := bm.DeleteContent(ctx, contentID); err != nil {
		t.Errorf("error deleting content: %v", contentID)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	dumpContents(ctx, t, bm, "after flush #2")
	dumpContentManagerData(ctx, t, data)
	// content still present in first pack
	verifyUnreferencedBlobsCount(ctx, t, bm, 0)

	assertNoError(t, bm.RewriteContent(ctx, contentID))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	verifyUnreferencedBlobsCount(ctx, t, bm, 1)
	assertNoError(t, bm.RewriteContent(ctx, contentID))

	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	verifyUnreferencedBlobsCount(ctx, t, bm, 2)
}

func TestFindUnreferencedBlobs2(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, nil)
	defer bm.Close(ctx)

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

func dumpContents(ctx context.Context, t *testing.T, bm *Manager, caption string) {
	t.Helper()

	count := 0

	log(ctx).Infof("dumping %v contents", caption)

	if err := bm.IterateContents(ctx, IterateOptions{IncludeDeleted: true},
		func(ci Info) error {
			log(ctx).Debugf(" ci[%v]=%#v", count, ci)
			count++
			return nil
		}); err != nil {
		t.Errorf("error listing contents: %v", err)
		return
	}

	log(ctx).Infof("finished dumping %v %v contents", count, caption)
}

func verifyUnreferencedBlobsCount(ctx context.Context, t *testing.T, bm *Manager, want int) {
	t.Helper()

	var unrefCount int32

	err := bm.IterateUnreferencedBlobs(ctx, nil, 1, func(_ blob.Metadata) error {
		atomic.AddInt32(&unrefCount, 1)
		return nil
	})
	if err != nil {
		t.Errorf("error in IterateUnreferencedBlobs: %v", err)
	}

	log(ctx).Infof("got %v expecting %v", unrefCount, want)

	if got := int(unrefCount); got != want {
		t.Fatalf("invalid number of unreferenced contents: %v, wanted %v", got, want)
	}
}

func TestContentWriteAliasing(t *testing.T) {
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, faketime.Frozen(fakeTime))
	defer bm.Close(ctx)

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
	ctx := testlogging.Context(t)
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	bm := newTestContentManager(t, data, keyTime, faketime.Frozen(fakeTime))
	defer bm.Close(ctx)

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
		writeVer := writeVer
		t.Run(fmt.Sprintf("version-%v", writeVer), func(t *testing.T) {
			verifyVersionCompat(t, writeVer)
		})
	}
}

func verifyVersionCompat(t *testing.T, writeVersion int) {
	ctx := testlogging.Context(t)

	// create content manager that writes 'writeVersion' and reads all versions >= minSupportedReadVersion
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	mgr := newTestContentManager(t, data, keyTime, nil)
	defer mgr.Close(ctx)

	mgr.writeFormatVersion = int32(writeVersion)

	dataSet := map[ID][]byte{}

	for i := 0; i < 3000000; i = (i + 1) * 2 {
		data := make([]byte, i)
		cryptorand.Read(data) //nolint:errcheck

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
		assertNoError(t, mgr.DeleteContent(ctx, blobID))
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
	mgr = newTestContentManager(t, data, keyTime, nil)
	defer mgr.Close(ctx)

	// make sure we can read everything
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	if err := mgr.CompactIndexes(ctx, CompactOptions{MaxSmallBlobs: 1}); err != nil {
		t.Fatalf("unable to compact indexes: %v", err)
	}

	if err := mgr.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	verifyContentManagerDataSet(ctx, t, mgr, dataSet)

	// now open one more manager
	mgr = newTestContentManager(t, data, keyTime, nil)
	defer mgr.Close(ctx)
	verifyContentManagerDataSet(ctx, t, mgr, dataSet)
}

func TestReadsOwnWritesWithEventualConsistencyPersistentOwnWritesCache(t *testing.T) {
	data := blobtesting.DataMap{}
	timeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, nil, timeNow)
	cacheData := blobtesting.DataMap{}
	cacheKeyTime := map[blob.ID]time.Time{}
	cacheSt := blobtesting.NewMapStorage(cacheData, cacheKeyTime, timeNow)
	ecst := blobtesting.NewEventuallyConsistentStorage(logging.NewWrapper(st, t.Logf, "[STORAGE] "), 0.1)

	// disable own writes cache, will still be ok if store is strongly consistent
	verifyReadsOwnWrites(t, ecst, timeNow, &persistentOwnWritesCache{
		st:      cacheSt,
		timeNow: timeNow,
	})
}

func TestReadsOwnWritesWithStrongConsistencyAndNoCaching(t *testing.T) {
	data := blobtesting.DataMap{}
	timeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, nil, timeNow)

	// if we used nullOwnWritesCache and eventual consistency, the test would fail
	// st = blobtesting.NewEventuallyConsistentStorage(logging.NewWrapper(st, t.Logf, "[STORAGE] "), 0.1)

	// disable own writes cache, will still be ok if store is strongly consistent
	verifyReadsOwnWrites(t, st, timeNow, &nullOwnWritesCache{})
}

func TestReadsOwnWritesWithEventualConsistencyInMemoryOwnWritesCache(t *testing.T) {
	data := blobtesting.DataMap{}
	timeNow := faketime.AutoAdvance(fakeTime, 1*time.Second)
	st := blobtesting.NewMapStorage(data, nil, timeNow)
	ecst := blobtesting.NewEventuallyConsistentStorage(logging.NewWrapper(st, t.Logf, "[STORAGE] "), 0.1)

	verifyReadsOwnWrites(t, ecst, timeNow, &memoryOwnWritesCache{timeNow: timeNow})
}

func verifyReadsOwnWrites(t *testing.T, st blob.Storage, timeNow func() time.Time, sharedOwnWritesCache ownWritesCache) {
	ctx := testlogging.Context(t)
	cachingOptions := &CachingOptions{
		ownWritesCache: sharedOwnWritesCache,
	}

	bm := newTestContentManagerWithStorageAndCaching(t, st, cachingOptions, timeNow)

	ids := make([]ID, 100)
	for i := 0; i < len(ids); i++ {
		ids[i] = writeContentAndVerify(ctx, t, bm, seededRandomData(i, maxPackCapacity/2))

		for j := 0; j < i; j++ {
			// verify all contents written so far
			verifyContent(ctx, t, bm, ids[j], seededRandomData(j, maxPackCapacity/2))
		}

		// every 10 contents, create new content manager
		if i%10 == 0 {
			t.Logf("------- reopening -----")
			must(t, bm.Close(ctx))
			bm = newTestContentManagerWithStorageAndCaching(t, st, cachingOptions, timeNow)
		}
	}

	must(t, bm.Close(ctx))
	bm = newTestContentManagerWithStorageAndCaching(t, st, cachingOptions, timeNow)

	for i := 0; i < len(ids); i++ {
		verifyContent(ctx, t, bm, ids[i], seededRandomData(i, maxPackCapacity/2))
	}
}

func verifyContentManagerDataSet(ctx context.Context, t *testing.T, mgr *Manager, dataSet map[ID][]byte) {
	for contentID, originalPayload := range dataSet {
		v, err := mgr.GetContent(ctx, contentID)
		if err != nil {
			t.Errorf("unable to read content %q: %v", contentID, err)
			continue
		}

		if !bytes.Equal(v, originalPayload) {
			t.Errorf("payload for %q does not match original: %v", v, originalPayload)
		}
	}
}

func newTestContentManager(t *testing.T, data blobtesting.DataMap, keyTime map[blob.ID]time.Time, timeFunc func() time.Time) *Manager {
	st := blobtesting.NewMapStorage(data, keyTime, timeFunc)
	return newTestContentManagerWithStorage(t, st, timeFunc)
}

func newTestContentManagerWithStorage(t *testing.T, st blob.Storage, timeFunc func() time.Time) *Manager {
	return newTestContentManagerWithStorageAndCaching(t, st, nil, timeFunc)
}

func newTestContentManagerWithStorageAndCaching(t *testing.T, st blob.Storage, co *CachingOptions, timeFunc func() time.Time) *Manager {
	if timeFunc == nil {
		timeFunc = faketime.AutoAdvance(fakeTime, 1*time.Second)
	}

	bm, err := newManagerWithOptions(testlogging.Context(t), st, &FormattingOptions{
		Hash:        "HMAC-SHA256",
		Encryption:  "AES256-GCM-HMAC-SHA256",
		HMACSecret:  hmacSecret,
		MaxPackSize: maxPackSize,
		Version:     1,
	}, co, timeFunc, nil)
	if err != nil {
		panic("can't create content manager: " + err.Error())
	}

	bm.checkInvariantsOnUnlock = true

	return bm
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
		t.Fatalf("unable to read content %q: %v", contentID, err)
		return
	}

	if got, want := b2, b; !bytes.Equal(got, want) {
		t.Errorf("content %q data mismatch: got %x (nil:%v), wanted %x (nil:%v)", contentID, got, got == nil, want, want == nil)
	}

	if _, err := bm.ContentInfo(ctx, contentID); err != nil {
		t.Errorf("error getting content info %q: %v", contentID, err)
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

func flushWithRetries(ctx context.Context, t *testing.T, bm *Manager) int {
	t.Helper()

	var retryCount int

	err := bm.Flush(ctx)
	for i := 0; err != nil && i < maxRetries; i++ {
		log(ctx).Warningf("flush failed %v, retrying", err)
		err = bm.Flush(ctx)
		retryCount++
	}

	if err != nil {
		t.Errorf("err: %v", err)
	}

	return retryCount
}

func writeContentWithRetriesAndVerify(ctx context.Context, t *testing.T, bm *Manager, b []byte) (contentID ID, retryCount int) {
	t.Helper()

	contentID, err := bm.WriteContent(ctx, b, "")
	for i := 0; err != nil && i < maxRetries; i++ {
		retryCount++

		log(ctx).Warningf("WriteContent failed %v, retrying", err)

		contentID, err = bm.WriteContent(ctx, b, "")
	}

	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := contentID, ID(hashValue(b)); got != want {
		t.Errorf("invalid content ID for %x, got %v, want %v", b, got, want)
	}

	verifyContent(ctx, t, bm, contentID, b)

	return contentID, retryCount
}

func seededRandomData(seed, length int) []byte {
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

func dumpContentManagerData(ctx context.Context, t *testing.T, data blobtesting.DataMap) {
	t.Helper()
	log(ctx).Infof("***data - %v items", len(data))

	for k, v := range data {
		if k[0] == 'n' {
			log(ctx).Infof("index %v (%v bytes)", k, len(v))
		} else {
			log(ctx).Infof("non-index %v (%v bytes)\n", k, len(v))
		}
	}

	log(ctx).Infof("*** end of data")
}

func makeRandomHexString(t *testing.T, length int) string {
	t.Helper()

	b := make([]byte, (length-1)/2+1)
	if _, err := rand.Read(b); err != nil { // nolint:gosec
		t.Fatal("Could not read random bytes", err)
	}

	return hex.EncodeToString(b)
}

func deleteContent(ctx context.Context, t *testing.T, bm *Manager, c ID) {
	t.Helper()

	if err := bm.DeleteContent(ctx, c); err != nil {
		t.Fatalf("Unable to delete content %v: %v", c, err)
	}
}

func getContentInfo(t *testing.T, bm *Manager, c ID) Info {
	t.Helper()

	_, i, err := bm.getContentInfo(c)
	if err != nil {
		t.Fatalf("Unable to get content info for %q: %v", c, err)
	}

	return i
}

func must(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}
