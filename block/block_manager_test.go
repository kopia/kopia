package block

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/internal/storagetesting"
	"github.com/kopia/kopia/storage"
)

const (
	maxPackSize = 2000
)

var fakeTime = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)

func init() {
	//zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func TestBlockManagerEmptyFlush(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	bm.Flush(ctx)
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockZeroBytes1(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	writeBlockAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	dumpBlockManagerData(data)
}

func TestBlockZeroBytes2(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 10))
	writeBlockAndVerify(ctx, t, bm, []byte{})
	bm.Flush(ctx)
	dumpBlockManagerData(data)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
		dumpBlockManagerData(data)
	}
}

func TestBlockManagerSmallBlockWrites(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
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
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
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
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
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

	// this flushes the pack block + index block
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	dumpBlockManagerData(data)
}

func TestBlockManagerEmpty(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	noSuchBlockID := md5hash([]byte("foo"))

	b, err := bm.GetBlock(ctx, noSuchBlockID)
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block: %v, %v", b, err)
	}

	bi, err := bm.BlockInfo(ctx, noSuchBlockID)
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block info: %v, %v", bi, err)
	}

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerRepack(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	// disable preamble, postamble and padding, so that each pack block is identical to its contents
	bm.maxPreambleLength = 0
	bm.minPreambleLength = 0
	bm.paddingUnit = 0

	d1 := seededRandomData(1, 10)
	d2 := seededRandomData(2, 20)
	d3 := seededRandomData(3, 30)

	writeBlockAndVerify(ctx, t, bm, d1)
	bm.Flush(ctx)
	writeBlockAndVerify(ctx, t, bm, d2)
	bm.Flush(ctx)
	writeBlockAndVerify(ctx, t, bm, d3)
	bm.Flush(ctx)

	// 3 data blocks, 3 index blocks.
	if got, want := len(data), 6; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
	}

	log.Printf("before repackage")
	dumpBlockManagerData(data)

	if err := bm.Repackage(ctx, 5); err != nil {
		t.Errorf("repackage failure: %v", err)
	}
	bm.Flush(ctx)

	// nothing happened, still 3 data blocks, 3 index blocks.
	if got, want := len(data), 6; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
	}

	bm.timeNow = fakeTimeNowFrozen(fakeTime.Add(1 * time.Second))

	if err := bm.Repackage(ctx, 30); err != nil {
		t.Errorf("repackage failure: %v", err)
	}
	bm.Flush(ctx)

	log.Printf("after repackage")
	dumpBlockManagerData(data)

	// added one more data block + one mode index block.
	if got, want := len(data), 8; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
	}
	if err := bm.CompactIndexes(ctx); err != nil {
		t.Errorf("compaction failure: %v", err)
	}

	if got, want := len(data), 9; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
		dumpBlockManagerData(data)
	}

	verifyActiveIndexBlockCount(ctx, t, bm, 1)
}

func verifyActiveIndexBlockCount(ctx context.Context, t *testing.T, bm *Manager, expected int) {
	t.Helper()

	blks, err := bm.ActiveIndexBlocks(ctx)
	if err != nil {
		t.Errorf("error listing active index blocks: %v", err)
		return
	}

	if got, want := len(blks), expected; got != want {
		t.Errorf("unexpected number of active index blocks %v, expected %v (%v)", got, want, blks)
	}
}
func TestBlockManagerInternalFlush(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
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
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	dumpBlockManagerData(data)
}

func TestBlockManagerWriteMultiple(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)

	var blockIDs []string

	for i := 0; i < 5000; i++ {
		//t.Logf("i=%v", i)
		b := seededRandomData(i, i%113)
		//t.Logf("writing block #%v with %x", i, b)
		blkID, err := bm.WriteBlock(ctx, b, "")
		//t.Logf("wrote %v=%v", i, blkID)
		if err != nil {
			t.Errorf("err: %v", err)
		}

		blockIDs = append(blockIDs, blkID)

		if i%17 == 0 {
			t.Logf("flushing %v", i)
			bm.Flush(ctx)
			//dumpBlockManagerData(data)
		}

		if i%41 == 0 {
			t.Logf("opening new manager: %v", i)
			bm.Flush(ctx)
			t.Logf("data block count: %v", len(data))
			//dumpBlockManagerData(data)
			bm = newTestBlockManager(data, keyTime, nil)
		}
	}

	for _, blockID := range blockIDs {
		_, err := bm.GetBlock(ctx, blockID)
		if err != nil {
			t.Errorf("can't read block %q: %v", blockID, err)
			continue
		}
	}
}

func TestBlockManagerConcurrency(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime, nil)
	preexistingBlock := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
	bm.Flush(ctx)

	bm1 := newTestBlockManager(data, keyTime, nil)
	bm2 := newTestBlockManager(data, keyTime, nil)
	bm3 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(1), 1))

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

	if err := bm4.CompactIndexes(ctx); err != nil {
		t.Errorf("compaction error: %v", err)
	}
	if got, want := getIndexCount(data), 5; got != want {
		t.Errorf("unexpected index count after partial compaction: %v, wanted %v", got, want)
	}

	// new block manager at this point can see all data.
	bm5 := newTestBlockManager(data, keyTime, nil)
	verifyBlock(ctx, t, bm5, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(ctx, t, bm5, sharedBlock, seededRandomData(20, 100))
	verifyBlock(ctx, t, bm5, bm1block, seededRandomData(31, 100))
	verifyBlock(ctx, t, bm5, bm2block, seededRandomData(32, 100))
	verifyBlock(ctx, t, bm5, bm3block, seededRandomData(33, 100))
	if err := bm5.CompactIndexes(ctx); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func TestDeleteBlock(t *testing.T) {
	ctx := context.Background()
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
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
	bm = newTestBlockManager(data, keyTime, nil)
	dumpBlockManagerData(data)
	verifyBlockNotFound(ctx, t, bm, block1)
	verifyBlockNotFound(ctx, t, bm, block2)
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
		{"deleted before delete and-recreate", fakeTime.Add(5), true},
		{"deleted after delete and recreate", fakeTime.Add(25), false},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			// write a block
			data := map[string][]byte{}
			keyTime := map[string]time.Time{}
			bm := newTestBlockManager(data, keyTime, fakeTimeNowFrozen(fakeTime))
			block1 := writeBlockAndVerify(ctx, t, bm, seededRandomData(10, 100))
			bm.Flush(ctx)

			// delete but at given timestamp but don't commit yet.
			bm0 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(tc.deletionTime, 1))
			bm0.DeleteBlock(block1)

			// delete it at t0+10
			bm1 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(10), 1))
			verifyBlock(ctx, t, bm1, block1, seededRandomData(10, 100))
			bm1.DeleteBlock(block1)
			bm1.Flush(ctx)

			// recreate at t0+20
			bm2 := newTestBlockManager(data, keyTime, fakeTimeNowWithAutoAdvance(fakeTime.Add(20), 1))
			block2 := writeBlockAndVerify(ctx, t, bm2, seededRandomData(10, 100))
			bm2.Flush(ctx)

			// commit deletion from bm0 (t0+5)
			bm0.Flush(ctx)

			dumpBlockManagerData(data)

			if block1 != block2 {
				t.Errorf("got invalid block %v, expected %v", block2, block1)
			}

			bm3 := newTestBlockManager(data, keyTime, nil)
			if tc.isVisible {
				verifyBlock(ctx, t, bm3, block1, seededRandomData(10, 100))
			} else {
				verifyBlockNotFound(ctx, t, bm3, block1)
			}
		})
	}
}

func newTestBlockManager(data map[string][]byte, keyTime map[string]time.Time, timeFunc func() time.Time) *Manager {
	st := storagetesting.NewMapStorage(data, keyTime)
	//st = logging.NewWrapper(st)
	if timeFunc == nil {
		timeFunc = fakeTimeNowWithAutoAdvance(fakeTime, 1)
	}
	bm, err := newManagerWithTime(context.Background(), st, FormattingOptions{
		BlockFormat: "TESTONLY_MD5",
		MaxPackSize: maxPackSize,
	}, CachingOptions{}, timeFunc)
	bm.checkInvariantsOnUnlock = true

	bm.maxInlineContentLength = 0
	if err != nil {
		panic("can't create block manager: " + err.Error())
	}
	return bm
}

func getIndexCount(d map[string][]byte) int {
	var cnt int

	for k := range d {
		if strings.HasPrefix(k, indexBlockPrefix) {
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
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected response from GetBlock(%q), got %v,%v, expected %v", blockID, b, err, storage.ErrBlockNotFound)
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

	if got, want := bi.Length, int64(len(b)); got != want {
		t.Errorf("invalid block size for %q: %v, wanted %v", blockID, got, want)
	}

}
func writeBlockAndVerify(ctx context.Context, t *testing.T, bm *Manager, b []byte) string {
	t.Helper()

	blockID, err := bm.WriteBlock(ctx, b, "")
	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := blockID, md5hash(b); got != want {
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

func md5hash(b []byte) string {
	h := md5.Sum(b)
	return hex.EncodeToString(h[:])
}

func dumpBlockManagerData(data map[string][]byte) {
	for k, v := range data {
		if k[0] == 'I' {
			var payload blockmgrpb.Indexes
			proto.Unmarshal(v, &payload)
			fmt.Printf("index %v:\n", k)
			for _, ndx := range payload.Indexes {
				fmt.Printf("  pack %v len: %v created %v\n", ndx.PackBlockId, ndx.PackLength, time.Unix(0, int64(ndx.CreateTimeNanos)).Local())
				for blk, os := range ndx.Items {
					off, size := unpackOffsetAndSize(os)
					fmt.Printf("    block[%v]={offset:%v size:%v}\n", blk, off, size)
				}
				for _, del := range ndx.DeletedItems {
					fmt.Printf("    deleted %v\n", del)
				}
			}
		} else {
			fmt.Printf("data %v (%v bytes)\n", k, len(v))
		}
	}
}
