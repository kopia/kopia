package block

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

func TestBlockManagerEmptyFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	bm.Flush(ctx)
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockZeroBytes1(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	blockID := writeBlockAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	dumpBlockManagerData(t, data)
	bm = newTestBlockManager(data, keyTime, nil)
	verifyBlock(ctx, t, bm, blockID, []byte{})
}

func TestBlockZeroBytes2(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 10))
	writeBlockAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
		dumpBlockManagerData(t, data)
	}
}

func TestBlockManagerSmallBlockWrites(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		writeBlockAndVerify(ctx, t, bm, seededRandomData(i, 10))
	}
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerDedupesPendingBlocks(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		writeBlockAndVerify(ctx, t, bm, seededRandomData(0, 999))
	}
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerDedupesPendingAndUncommittedBlocks(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	// no writes here, all data fits in a single pack.
	writeBlockAndVerify(ctx, t, bm, seededRandomData(0, 950))
	writeBlockAndVerify(ctx, t, bm, seededRandomData(1, 950))
	writeBlockAndVerify(ctx, t, bm, seededRandomData(2, 10))
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	// no writes here
	writeBlockAndVerify(ctx, t, bm, seededRandomData(0, 950))
	writeBlockAndVerify(ctx, t, bm, seededRandomData(1, 950))
	writeBlockAndVerify(ctx, t, bm, seededRandomData(2, 10))
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	bm.Flush(ctx)

	// this flushes the pack block + index blob
	if got, want := len(data), 2; got != want {
		dumpBlockManagerData(t, data)
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerEmpty(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	noSuchBlockID := string(hashValue([]byte("foo")))

	b, err := bm.GetBlock(ctx, noSuchBlockID)
	if err != ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block: %v, %v", b, err)
	}

	bi, err := bm.BlockInfo(ctx, noSuchBlockID)
	if err != ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block info: %v, %v", bi, err)
	}

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
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
func TestBlockManagerInternalFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		rand.Read(b)
		writeBlockAndVerify(ctx, t, bm, b)
	}

	// 1 data block written, but no index yet.
	if got, want := len(data), 1; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	// do it again - should be 2 blocks + 1000 bytes pending.
	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		rand.Read(b)
		writeBlockAndVerify(ctx, t, bm, b)
	}

	// 2 data blocks written, but no index yet.
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	bm.Flush(ctx)

	// third block gets written, followed by index.
	if got, want := len(data), 4; got != want {
		dumpBlockManagerData(t, data)
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerWriteMultiple(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	timeFunc := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
	bm := newTestBlockManager(data, keyTime, timeFunc)

	var blockIDs []string

	for i := 0; i < 5000; i++ {
		//t.Logf("i=%v", i)
		b := seededRandomData(i, i%113)
		blkID, err := bm.WriteBlock(ctx, b, "")
		if err != nil {
			t.Errorf("err: %v", err)
		}

		blockIDs = append(blockIDs, blkID)

		if i%17 == 0 {
			//t.Logf("flushing %v", i)
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}
			//dumpBlockManagerData(t, data)
		}

		if i%41 == 0 {
			//t.Logf("opening new manager: %v", i)
			if err := bm.Flush(ctx); err != nil {
				t.Fatalf("error flushing: %v", err)
			}
			//t.Logf("data block count: %v", len(data))
			//dumpBlockManagerData(t, data)
			bm = newTestBlockManager(data, keyTime, timeFunc)
		}

		pos := rand.Intn(len(blockIDs))
		if _, err := bm.GetBlock(ctx, blockIDs[pos]); err != nil {
			dumpBlockManagerData(t, data)
			t.Fatalf("can't read block %q: %v", blockIDs[pos], err)
			continue
		}
	}
}

// This is regression test for a bug where we would corrupt data when encryption
// was done in place and clobbered pending data in memory.
func TestBlockManagerFailedToWritePack(t *testing.T) {
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
		"PutBlock": {
			{Err: errors.New("booboo")},
		},
	}

	b1, err := bm.WriteBlock(ctx, seededRandomData(1, 10), "")
	if err != nil {
		t.Fatalf("can't create block: %v", err)
	}

	if err := bm.Flush(ctx); err != nil {
		t.Logf("expected flush error: %v", err)
	}

	verifyBlock(ctx, t, bm, b1, seededRandomData(1, 10))
}

func TestBlockManagerConcurrency(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	preexistingBlock := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)

	dumpBlockManagerData(t, data)
	bm1 := newTestBlockManager(data, keyTime, nil)
	bm2 := newTestBlockManager(data, keyTime, nil)
	bm3 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(1), 1*time.Second))

	// all bm* can see pre-existing block
	verifyBlock(ctx, t, bm1, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(ctx, t, bm2, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(ctx, t, bm3, preexistingBlock, seededRandomData(10, 100))

	// write the same block in all managers.
	sharedBlock := writeBlockAndVerify(ctx, t, bm1, seededRandomData(20, 100))
	writeBlockAndVerify(ctx, t, bm2, seededRandomData(20, 100))
	writeBlockAndVerify(ctx, t, bm3, seededRandomData(20, 100))

	// write unique block per manager.
	bm1block := writeBlockAndVerify(ctx, t, bm1, seededRandomData(31, 100))
	bm2block := writeBlockAndVerify(ctx, t, bm2, seededRandomData(32, 100))
	bm3block := writeBlockAndVerify(ctx, t, bm3, seededRandomData(33, 100))

	// make sure they can't see each other's unflushed blocks.
	verifyBlockNotFound(ctx, t, bm1, bm2block)
	verifyBlockNotFound(ctx, t, bm1, bm3block)
	verifyBlockNotFound(ctx, t, bm2, bm1block)
	verifyBlockNotFound(ctx, t, bm2, bm3block)
	verifyBlockNotFound(ctx, t, bm3, bm1block)
	verifyBlockNotFound(ctx, t, bm3, bm2block)

	// now flush all writers, they still can't see each others' data.
	bm1.Flush(ctx)
	bm2.Flush(ctx)
	bm3.Flush(ctx)
	verifyBlockNotFound(ctx, t, bm1, bm2block)
	verifyBlockNotFound(ctx, t, bm1, bm3block)
	verifyBlockNotFound(ctx, t, bm2, bm1block)
	verifyBlockNotFound(ctx, t, bm2, bm3block)
	verifyBlockNotFound(ctx, t, bm3, bm1block)
	verifyBlockNotFound(ctx, t, bm3, bm2block)

	// new block manager at this point can see all data.
	bm4 := newTestBlockManager(data, keyTime, nil)
	verifyBlock(ctx, t, bm4, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(ctx, t, bm4, sharedBlock, seededRandomData(20, 100))
	verifyBlock(ctx, t, bm4, bm1block, seededRandomData(31, 100))
	verifyBlock(ctx, t, bm4, bm2block, seededRandomData(32, 100))
	verifyBlock(ctx, t, bm4, bm3block, seededRandomData(33, 100))

	if got, want := getIndexCount(data), 4; got != want {
		t.Errorf("unexpected index count before compaction: %v, wanted %v", got, want)
	}

	if err := bm4.CompactIndexes(ctx, CompactOptions{
		MinSmallBlocks: 1,
		MaxSmallBlocks: 1,
	}); err != nil {
		t.Errorf("compaction error: %v", err)
	}
	if got, want := getIndexCount(data), 1; got != want {
		t.Errorf("unexpected index count after compaction: %v, wanted %v", got, want)
	}

	// new block manager at this point can see all data.
	bm5 := newTestBlockManager(data, keyTime, nil)
	verifyBlock(ctx, t, bm5, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(ctx, t, bm5, sharedBlock, seededRandomData(20, 100))
	verifyBlock(ctx, t, bm5, bm1block, seededRandomData(31, 100))
	verifyBlock(ctx, t, bm5, bm2block, seededRandomData(32, 100))
	verifyBlock(ctx, t, bm5, bm3block, seededRandomData(33, 100))
	if err := bm5.CompactIndexes(ctx, CompactOptions{
		MinSmallBlocks: 1,
		MaxSmallBlocks: 1,
	}); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func TestDeleteBlock(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	block1 := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)
	block2 := writeBlockAndVerify(ctx, t, bm, seededRandomData(11, 100))
	if err := bm.DeleteBlock(block1); err != nil {
		t.Errorf("unable to delete block: %v", block1)
	}
	if err := bm.DeleteBlock(block2); err != nil {
		t.Errorf("unable to delete block: %v", block1)
	}
	verifyBlockNotFound(ctx, t, bm, block1)
	verifyBlockNotFound(ctx, t, bm, block2)
	bm.Flush(ctx)
	log.Debugf("-----------")
	bm = newTestBlockManager(data, keyTime, nil)
	//dumpBlockManagerData(t, data)
	verifyBlockNotFound(ctx, t, bm, block1)
	verifyBlockNotFound(ctx, t, bm, block2)
}

func TestRewriteNonDeleted(t *testing.T) {
	const stepBehaviors = 3

	// perform a sequence WriteBlock() <action1> RewriteBlock() <action2> GetBlock()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			t.Run(fmt.Sprintf("case-%v-%v", action1, action2), func(t *testing.T) {
				ctx := context.Background()
				data := blobtesting.DataMap{}
				keyTime := map[blob.ID]time.Time{}
				fakeNow := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
				bm := newTestBlockManager(data, keyTime, fakeNow)

				applyStep := func(action int) {
					switch action {
					case 0:
						t.Logf("flushing and reopening")
						bm.Flush(ctx)
						bm = newTestBlockManager(data, keyTime, fakeNow)
					case 1:
						t.Logf("flushing")
						bm.Flush(ctx)
					case 2:
						t.Logf("doing nothing")
					}
				}

				block1 := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
				applyStep(action1)
				assertNoError(t, bm.RewriteBlock(ctx, block1))
				applyStep(action2)
				verifyBlock(ctx, t, bm, block1, seededRandomData(10, 100))
				dumpBlockManagerData(t, data)
			})
		}
	}
}

func TestDisableFlush(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	bm.DisableIndexFlush()
	bm.DisableIndexFlush()
	for i := 0; i < 500; i++ {
		writeBlockAndVerify(ctx, t, bm, seededRandomData(i, 100))
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

	// perform a sequence WriteBlock() <action1> Delete() <action2> RewriteBlock() <action3> GetBlock()
	// where actionX can be (0=flush and reopen, 1=flush, 2=nothing)
	for action1 := 0; action1 < stepBehaviors; action1++ {
		for action2 := 0; action2 < stepBehaviors; action2++ {
			for action3 := 0; action3 < stepBehaviors; action3++ {
				t.Run(fmt.Sprintf("case-%v-%v-%v", action1, action2, action3), func(t *testing.T) {
					ctx := context.Background()
					data := blobtesting.DataMap{}
					keyTime := map[blob.ID]time.Time{}
					fakeNow := fakeTimeNowWithAutoAdvance(fakeTime, 1*time.Second)
					bm := newTestBlockManager(data, keyTime, fakeNow)

					applyStep := func(action int) {
						switch action {
						case 0:
							t.Logf("flushing and reopening")
							bm.Flush(ctx)
							bm = newTestBlockManager(data, keyTime, fakeNow)
						case 1:
							t.Logf("flushing")
							bm.Flush(ctx)
						case 2:
							t.Logf("doing nothing")
						}
					}

					block1 := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
					applyStep(action1)
					assertNoError(t, bm.DeleteBlock(block1))
					applyStep(action2)
					if got, want := bm.RewriteBlock(ctx, block1), ErrBlockNotFound; got != want && got != nil {
						t.Errorf("unexpected error %v, wanted %v", got, want)
					}
					applyStep(action3)
					verifyBlockNotFound(ctx, t, bm, block1)
					dumpBlockManagerData(t, data)
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
			// write a block
			data := blobtesting.DataMap{}
			keyTime := map[blob.ID]time.Time{}
			bm := newTestBlockManager(data, keyTime, fakeTimeNowFrozen(fakeTime))
			block1 := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
			bm.Flush(ctx)

			// delete but at given timestamp but don't commit yet.
			bm0 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(tc.deletionTime, 1*time.Second))
			assertNoError(t, bm0.DeleteBlock(block1))

			// delete it at t0+10
			bm1 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(10*time.Second), 1*time.Second))
			verifyBlock(ctx, t, bm1, block1, seededRandomData(10, 100))
			assertNoError(t, bm1.DeleteBlock(block1))
			bm1.Flush(ctx)

			// recreate at t0+20
			bm2 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(20*time.Second), 1*time.Second))
			block2 := writeBlockAndVerify(ctx, t, bm2, seededRandomData(10, 100))
			bm2.Flush(ctx)

			// commit deletion from bm0 (t0+5)
			bm0.Flush(ctx)

			//dumpBlockManagerData(t, data)

			if block1 != block2 {
				t.Errorf("got invalid block %v, expected %v", block2, block1)
			}

			bm3 := newTestBlockManager(data, keyTime, nil)
			dumpBlockManagerData(t, data)
			if tc.isVisible {
				verifyBlock(ctx, t, bm3, block1, seededRandomData(10, 100))
			} else {
				verifyBlockNotFound(ctx, t, bm3, block1)
			}
		})
	}
}

func TestFindUnreferencedBlobs(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	blockID := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	if err := bm.DeleteBlock(blockID); err != nil {
		t.Errorf("error deleting block: %v", blockID)
	}
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}

	// block still present in first pack
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)

	assertNoError(t, bm.RewriteBlock(ctx, blockID))
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 1)
	assertNoError(t, bm.RewriteBlock(ctx, blockID))
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 2)
}

func TestFindUnreferencedBlobs2(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	blockID := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
	writeBlockAndVerify(ctx, t, bm, seededRandomData(11, 100))
	dumpBlocks(t, bm, "after writing")
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	dumpBlocks(t, bm, "after flush")
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
	if err := bm.DeleteBlock(blockID); err != nil {
		t.Errorf("error deleting block: %v", blockID)
	}
	dumpBlocks(t, bm, "after delete")
	if err := bm.Flush(ctx); err != nil {
		t.Errorf("flush error: %v", err)
	}
	dumpBlocks(t, bm, "after flush")
	// block present in first pack, original pack is still referenced
	verifyUnreferencedStorageFilesCount(ctx, t, bm, 0)
}

func dumpBlocks(t *testing.T, bm *Manager, caption string) {
	t.Helper()
	infos, err := bm.ListBlockInfos("", true)
	if err != nil {
		t.Errorf("error listing blocks: %v", err)
		return
	}

	log.Infof("**** dumping %v blocks %v", len(infos), caption)
	for i, bi := range infos {
		log.Debugf(" bi[%v]=%#v", i, bi)
	}
	log.Infof("finished dumping %v blocks", len(infos))
}

func verifyUnreferencedStorageFilesCount(ctx context.Context, t *testing.T, bm *Manager, want int) {
	t.Helper()
	unref, err := bm.FindUnreferencedBlobs(ctx)
	if err != nil {
		t.Errorf("error in FindUnreferencedBlobs: %v", err)
	}

	log.Infof("got %v expecting %v", unref, want)
	if got := len(unref); got != want {
		t.Errorf("invalid number of unreferenced blocks: %v, wanted %v", got, want)
	}
}

func TestBlockWriteAliasing(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, fakeTimeNowFrozen(fakeTime))

	blockData := []byte{100, 0, 0}
	id1 := writeBlockAndVerify(ctx, t, bm, blockData)
	blockData[0] = 101
	id2 := writeBlockAndVerify(ctx, t, bm, blockData)
	bm.Flush(ctx)
	blockData[0] = 102
	id3 := writeBlockAndVerify(ctx, t, bm, blockData)
	blockData[0] = 103
	id4 := writeBlockAndVerify(ctx, t, bm, blockData)
	verifyBlock(ctx, t, bm, id1, []byte{100, 0, 0})
	verifyBlock(ctx, t, bm, id2, []byte{101, 0, 0})
	verifyBlock(ctx, t, bm, id3, []byte{102, 0, 0})
	verifyBlock(ctx, t, bm, id4, []byte{103, 0, 0})
}

func TestBlockReadAliasing(t *testing.T) {
	ctx := context.Background()
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	bm := newTestBlockManager(data, keyTime, fakeTimeNowFrozen(fakeTime))

	blockData := []byte{100, 0, 0}
	id1 := writeBlockAndVerify(ctx, t, bm, blockData)
	blockData2, err := bm.GetBlock(ctx, id1)
	if err != nil {
		t.Fatalf("can't get block data: %v", err)
	}

	blockData2[0]++
	verifyBlock(ctx, t, bm, id1, blockData)
	bm.Flush(ctx)
	verifyBlock(ctx, t, bm, id1, blockData)
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

	// create block manager that writes 'writeVersion' and reads all versions >= minSupportedReadVersion
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}
	mgr := newTestBlockManager(data, keyTime, nil)
	mgr.writeFormatVersion = int32(writeVersion)

	dataSet := map[string][]byte{}

	for i := 0; i < 3000000; i = (i + 1) * 2 {
		data := make([]byte, i)
		rand.Read(data)

		cid, err := mgr.WriteBlock(ctx, data, "")
		if err != nil {
			t.Fatalf("unable to write %v bytes: %v", len(data), err)
		}
		dataSet[cid] = data
	}
	verifyBlockManagerDataSet(ctx, t, mgr, dataSet)

	// delete random 3 items (map iteration order is random)
	cnt := 0
	for blobID := range dataSet {
		t.Logf("deleting %v", blobID)
		assertNoError(t, mgr.DeleteBlock(blobID))
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
	mgr = newTestBlockManager(data, keyTime, nil)

	// make sure we can read everything
	verifyBlockManagerDataSet(ctx, t, mgr, dataSet)

	if err := mgr.CompactIndexes(ctx, CompactOptions{
		MinSmallBlocks: 1,
		MaxSmallBlocks: 1,
	}); err != nil {
		t.Fatalf("unable to compact indexes: %v", err)
	}
	if err := mgr.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}
	verifyBlockManagerDataSet(ctx, t, mgr, dataSet)

	// now open one more manager
	mgr = newTestBlockManager(data, keyTime, nil)
	verifyBlockManagerDataSet(ctx, t, mgr, dataSet)
}

func verifyBlockManagerDataSet(ctx context.Context, t *testing.T, mgr *Manager, dataSet map[string][]byte) {
	for blockID, originalPayload := range dataSet {
		v, err := mgr.GetBlock(ctx, blockID)
		if err != nil {
			t.Errorf("unable to read block %q: %v", blockID, err)
			continue
		}

		if !reflect.DeepEqual(v, originalPayload) {
			t.Errorf("payload for %q does not match original: %v", v, originalPayload)
		}
	}
}

func newTestBlockManager(data blobtesting.DataMap, keyTime map[blob.ID]time.Time, timeFunc func() time.Time) *Manager {
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
		panic("can't create block manager: " + err.Error())
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

func verifyBlockNotFound(ctx context.Context, t *testing.T, bm *Manager, blockID string) {
	t.Helper()

	b, err := bm.GetBlock(ctx, blockID)
	if err != ErrBlockNotFound {
		t.Errorf("unexpected response from GetBlock(%q), got %v,%v, expected %v", blockID, b, err, ErrBlockNotFound)
	}
}

func verifyBlock(ctx context.Context, t *testing.T, bm *Manager, blockID string, b []byte) {
	t.Helper()

	b2, err := bm.GetBlock(ctx, blockID)
	if err != nil {
		t.Errorf("unable to read block %q: %v", blockID, err)
		return
	}

	if got, want := b2, b; !reflect.DeepEqual(got, want) {
		t.Errorf("block %q data mismatch: got %x (nil:%v), wanted %x (nil:%v)", blockID, got, got == nil, want, want == nil)
	}

	bi, err := bm.BlockInfo(ctx, blockID)
	if err != nil {
		t.Errorf("error getting block info %q: %v", blockID, err)
	}

	if got, want := bi.Length, uint32(len(b)); got != want {
		t.Errorf("invalid block size for %q: %v, wanted %v", blockID, got, want)
	}

}
func writeBlockAndVerify(ctx context.Context, t *testing.T, bm *Manager, b []byte) string {
	t.Helper()

	blockID, err := bm.WriteBlock(ctx, b, "")
	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := blockID, string(hashValue(b)); got != want {
		t.Errorf("invalid block ID for %x, got %v, want %v", b, got, want)
	}

	verifyBlock(ctx, t, bm, blockID, b)

	return blockID
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

func dumpBlockManagerData(t *testing.T, data blobtesting.DataMap) {
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
