package block

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kopia/kopia/internal/blockmgrpb"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/internal/storagetesting"
	"github.com/kopia/kopia/storage"
)

const (
	maxPackedContentLength = 1000
	maxPackSize            = 2000
)

var fakeTime = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)

func TestBlockManagerEmptyFlush(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)
	bm.Flush()
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockZeroBytes1(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)
	writeBlockAndVerify(t, bm, "", []byte{})
	bm.Flush()
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	dumpBlockManagerData(data)
}

func TestBlockZeroBytes2(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)
	writeBlockAndVerify(t, bm, "", seededRandomData(10, 10))
	writeBlockAndVerify(t, bm, "", []byte{})
	bm.Flush()
	dumpBlockManagerData(data)
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
		dumpBlockManagerData(data)
	}
}

func TestBlockManagerSmallBlockWrites(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	for i := 0; i < 100; i++ {
		writeBlockAndVerify(t, bm, "", seededRandomData(i, 10))
	}
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	bm.Flush()
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerUnpackedBlockWrites(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	for i := 0; i < 100; i++ {
		writeBlockAndVerify(t, bm, "", seededRandomData(i, 1001))
	}

	log.Printf("writing again")
	// make sure deduping works.
	for i := 0; i < 100; i++ {
		writeBlockAndVerify(t, bm, "", seededRandomData(i, 1001))
	}
	t.Logf("finished writing again")
	if got, want := len(data), 100; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	bm.Flush()
	if got, want := len(data), 101; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerDedupesPendingBlocks(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	for i := 0; i < 100; i++ {
		writeBlockAndVerify(t, bm, "", seededRandomData(0, maxPackedContentLength-1))
	}
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	bm.Flush()
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerDedupesPendingAndUncommittedBlocks(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	writeBlockAndVerify(t, bm, "", seededRandomData(0, 999))
	writeBlockAndVerify(t, bm, "", seededRandomData(1, 999))
	writeBlockAndVerify(t, bm, "", seededRandomData(2, 10))
	if got, want := len(data), 1; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	// no writes
	writeBlockAndVerify(t, bm, "", seededRandomData(0, 999))
	writeBlockAndVerify(t, bm, "", seededRandomData(1, 999))
	writeBlockAndVerify(t, bm, "", seededRandomData(2, 10))
	if got, want := len(data), 1; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	bm.Flush()
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	dumpBlockManagerData(data)
}

func TestBlockManagerEmpty(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	noSuchBlockID := md5hash([]byte("foo"))

	b, err := bm.GetBlock(noSuchBlockID)
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block: %v, %v", b, err)
	}

	bi, err := bm.BlockInfo(noSuchBlockID)
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block info: %v, %v", bi, err)
	}

	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockManagerPackIdentialToRawObject(t *testing.T) {
	data0 := []byte{}
	data1 := seededRandomData(1, 600)
	data2 := seededRandomData(2, 600)
	data3 := append(append([]byte(nil), data1...), data2...)

	b0 := md5hash(data0)
	b1 := md5hash(data1)
	b2 := md5hash(data2)
	b3 := md5hash(data3)

	t.Logf("data0 hash: %v", b0)
	t.Logf("data1 hash: %v", b1)
	t.Logf("data2 hash: %v", b2)
	t.Logf("data3 hash: %v", b3)

	cases := []struct {
		ordering           [][]byte
		expectedBlockCount int
	}{
		{ordering: [][]byte{data1, data2, data3, data0}, expectedBlockCount: 2},
		{ordering: [][]byte{data0, data1, data2, data3}, expectedBlockCount: 2},
		{ordering: [][]byte{data1, data0, data2, data3}, expectedBlockCount: 2},
		{ordering: [][]byte{data0, data1, data0, data2, data3}, expectedBlockCount: 2},
		{ordering: [][]byte{data0, data1, data0, data2, data3, data0}, expectedBlockCount: 2},
		{ordering: [][]byte{data1, data0, data2, nil, data0, data3}, expectedBlockCount: 3},
		{ordering: [][]byte{data1, data2, nil, data0, data3}, expectedBlockCount: 4},
		{ordering: [][]byte{data3, nil, data1, data2, data0}, expectedBlockCount: 3},
		{ordering: [][]byte{data3, data1, data2, data0}, expectedBlockCount: 2},
		{ordering: [][]byte{data3, data0, data1, data2}, expectedBlockCount: 2},
		{ordering: [][]byte{data3, data1, data0, data2}, expectedBlockCount: 2},
		{ordering: [][]byte{data3, data0, data1, data0, data2, data0}, expectedBlockCount: 2},
	}

	for i, tc := range cases {
		data := map[string][]byte{}
		keyTime := map[string]time.Time{}
		bm := newTestBlockManager(data, keyTime)

		t.Run(fmt.Sprintf("case-%v", i), func(t *testing.T) {
			for _, b := range tc.ordering {
				if b == nil {
					bm.Flush()
					continue
				}

				t.Logf("writing %v", md5hash(b))
				writeBlockAndVerify(t, bm, "some-group", b)
			}

			verifyBlock(t, bm, b0, data0)
			verifyBlock(t, bm, b1, data1)
			verifyBlock(t, bm, b2, data2)
			verifyBlock(t, bm, b3, data3)
			bm.Flush()
			dumpBlockManagerData(data)
			verifyBlock(t, bm, b0, data0)
			verifyBlock(t, bm, b1, data1)
			verifyBlock(t, bm, b2, data2)
			verifyBlock(t, bm, b3, data3)
			bm.Flush()

			// 2 data blocks written.
			if got, want := len(data), tc.expectedBlockCount; got != want {
				t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
			}
		})
	}
}

func TestBlockManagerRepack(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	d1 := seededRandomData(1, 10)
	d2 := seededRandomData(2, 20)
	d3 := seededRandomData(3, 30)

	writeBlockAndVerify(t, bm, "g1", d1)
	bm.Flush()
	writeBlockAndVerify(t, bm, "g1", d2)
	bm.Flush()
	writeBlockAndVerify(t, bm, "g1", d3)
	bm.Flush()

	// 3 data blocks, 3 index blocks.
	if got, want := len(data), 6; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
	}

	if err := bm.Repackage("g1", 5); err != nil {
		t.Errorf("repackage failure: %v", err)
	}
	bm.Flush()

	// nothing happened, still 3 data blocks, 3 index blocks.
	if got, want := len(data), 6; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
	}

	setFakeTime(bm, fakeTime.Add(1*time.Second))

	if err := bm.Repackage("g1", 30); err != nil {
		t.Errorf("repackage failure: %v", err)
	}
	bm.Flush()
	log.Printf("after repackage")
	dumpBlockManagerData(data)

	// added one more data block + one mode index block.
	if got, want := len(data), 8; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
	}
	if err := bm.CompactIndexes(); err != nil {
		t.Errorf("compaction failure: %v", err)
	}

	if got, want := len(data), 9; got != want {
		t.Errorf("unexpected block count: %v, wanted %v", got, want)
		dumpBlockManagerData(data)
	}

	verifyActiveIndexBlockCount(t, bm, 1)
}

func verifyActiveIndexBlockCount(t *testing.T, bm *Manager, expected int) {
	t.Helper()

	blks, err := bm.ActiveIndexBlocks()
	if err != nil {
		t.Errorf("error listing active index blocks: %v", err)
		return
	}

	if got, want := len(blks), expected; got != want {
		t.Errorf("unexpected number of active index blocks %v, expected %v (%v)", got, want, blks)
	}
}
func TestBlockManagerInternalFlush(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		rand.Read(b)
		writeBlockAndVerify(t, bm, "", b)
	}

	// 1 data block written, but no index yet.
	if got, want := len(data), 1; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	// do it again - should be 2 blocks + 1000 bytes pending.
	for i := 0; i < 100; i++ {
		b := make([]byte, 25)
		rand.Read(b)
		writeBlockAndVerify(t, bm, "", b)
	}

	// 2 data blocks written, but no index yet.
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	bm.Flush()

	// third block gets written, followed by index.
	if got, want := len(data), 4; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}

	dumpBlockManagerData(data)
}

func TestBlockManagerWriteMultiple(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)

	var blockIDs []string

	for i := 0; i < 5000; i++ {
		//t.Logf("i=%v", i)
		b := seededRandomData(i, i%113)
		//t.Logf("writing block #%v with %x", i, b)
		blkID, err := bm.WriteBlock("", b)
		//t.Logf("wrote %v=%v", i, blkID)
		if err != nil {
			t.Errorf("err: %v", err)
		}

		blockIDs = append(blockIDs, blkID)

		if i%17 == 0 {
			t.Logf("flushing %v", i)
			bm.Flush()
			//dumpBlockManagerData(data)
		}

		if i%41 == 0 {
			t.Logf("opening new manager: %v", i)
			bm.Flush()
			t.Logf("data block count: %v", len(data))
			//dumpBlockManagerData(data)
			bm = newTestBlockManager(data, keyTime)
		}
	}

	for _, blockID := range blockIDs {
		_, err := bm.GetBlock(blockID)
		if err != nil {
			t.Errorf("can't read block %q: %v", blockID, err)
			continue
		}
	}
}

func TestBlockManagerListGroups(t *testing.T) {
	blockSizes := []int{10, 1500}

	for _, blockSize := range blockSizes {
		blockSize := blockSize
		t.Run(fmt.Sprintf("block-size-%v", blockSize), func(t *testing.T) {
			data := map[string][]byte{}
			keyTime := map[string]time.Time{}
			bm := newTestBlockManager(data, keyTime)
			data1 := seededRandomData(1, blockSize)
			data2 := seededRandomData(2, blockSize)
			data3 := seededRandomData(3, blockSize)

			writeBlockAndVerify(t, bm, "group1", data1)
			writeBlockAndVerify(t, bm, "group1", data2)
			writeBlockAndVerify(t, bm, "group1", data3)

			writeBlockAndVerify(t, bm, "group2", data1)
			writeBlockAndVerify(t, bm, "group2", data3)

			writeBlockAndVerify(t, bm, "group3", data1)
			writeBlockAndVerify(t, bm, "group3", data2)

			writeBlockAndVerify(t, bm, "group4", data2)
			writeBlockAndVerify(t, bm, "group4", data3)

			verifyGroupListContains(t, bm, "group1", md5hash(data1), md5hash(data2), md5hash(data3))
			verifyGroupListContains(t, bm, "group2", md5hash(data1), md5hash(data3))
			verifyGroupListContains(t, bm, "group3", md5hash(data1), md5hash(data2))
			verifyGroupListContains(t, bm, "group4", md5hash(data2), md5hash(data3))

			bm.Flush()

			data1b := seededRandomData(11, blockSize)
			data2b := seededRandomData(12, blockSize)
			data3b := seededRandomData(13, blockSize)

			bm = newTestBlockManager(data, keyTime)
			writeBlockAndVerify(t, bm, "group1", data1b)
			writeBlockAndVerify(t, bm, "group1", data2b)
			writeBlockAndVerify(t, bm, "group1", data3b)

			writeBlockAndVerify(t, bm, "group2", data1b)
			writeBlockAndVerify(t, bm, "group2", data3b)

			writeBlockAndVerify(t, bm, "group3", data1b)
			writeBlockAndVerify(t, bm, "group3", data2b)

			writeBlockAndVerify(t, bm, "group4", data2b)
			writeBlockAndVerify(t, bm, "group4", data3b)

			verifyGroupListContains(t, bm, "group1", md5hash(data1), md5hash(data2), md5hash(data3), md5hash(data1b), md5hash(data2b), md5hash(data3b))
			verifyGroupListContains(t, bm, "group2", md5hash(data1), md5hash(data3), md5hash(data1b), md5hash(data3b))
			verifyGroupListContains(t, bm, "group3", md5hash(data1), md5hash(data2), md5hash(data1b), md5hash(data2b))
			verifyGroupListContains(t, bm, "group4", md5hash(data2), md5hash(data3), md5hash(data2b), md5hash(data3b))
			bm.Flush()
			bm = newTestBlockManager(data, keyTime)
			verifyGroupListContains(t, bm, "group1", md5hash(data1), md5hash(data2), md5hash(data3), md5hash(data1b), md5hash(data2b), md5hash(data3b))
			verifyGroupListContains(t, bm, "group2", md5hash(data1), md5hash(data3), md5hash(data1b), md5hash(data3b))
			verifyGroupListContains(t, bm, "group3", md5hash(data1), md5hash(data2), md5hash(data1b), md5hash(data2b))
			verifyGroupListContains(t, bm, "group4", md5hash(data2), md5hash(data3), md5hash(data2b), md5hash(data3b))

			dumpBlockManagerData(data)
		})
	}
}

func TestBlockManagerConcurrency(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)
	preexistingBlock := writeBlockAndVerify(t, bm, "", seededRandomData(10, 100))
	bm.Flush()

	bm1 := newTestBlockManager(data, keyTime)
	bm2 := newTestBlockManager(data, keyTime)
	bm3 := newTestBlockManager(data, keyTime)
	setFakeTime(bm3, fakeTime.Add(1))

	// all bm* can see pre-existing block
	verifyBlock(t, bm1, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(t, bm2, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(t, bm3, preexistingBlock, seededRandomData(10, 100))

	// write the same block in all managers.
	sharedBlock := writeBlockAndVerify(t, bm1, "", seededRandomData(20, 100))
	writeBlockAndVerify(t, bm2, "", seededRandomData(20, 100))
	writeBlockAndVerify(t, bm3, "", seededRandomData(20, 100))

	// write unique block per manager.
	bm1block := writeBlockAndVerify(t, bm1, "", seededRandomData(31, 100))
	bm2block := writeBlockAndVerify(t, bm2, "", seededRandomData(32, 100))
	bm3block := writeBlockAndVerify(t, bm3, "", seededRandomData(33, 100))

	// make sure they can't see each other's unflushed blocks.
	verifyBlockNotFound(t, bm1, bm2block)
	verifyBlockNotFound(t, bm1, bm3block)
	verifyBlockNotFound(t, bm2, bm1block)
	verifyBlockNotFound(t, bm2, bm3block)
	verifyBlockNotFound(t, bm3, bm1block)
	verifyBlockNotFound(t, bm3, bm2block)

	// now flush all writers, they still can't see each others' data.
	bm1.Flush()
	bm2.Flush()
	bm3.Flush()
	verifyBlockNotFound(t, bm1, bm2block)
	verifyBlockNotFound(t, bm1, bm3block)
	verifyBlockNotFound(t, bm2, bm1block)
	verifyBlockNotFound(t, bm2, bm3block)
	verifyBlockNotFound(t, bm3, bm1block)
	verifyBlockNotFound(t, bm3, bm2block)

	// new block manager at this point can see all data.
	bm4 := newTestBlockManager(data, keyTime)
	verifyBlock(t, bm4, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(t, bm4, sharedBlock, seededRandomData(20, 100))
	verifyBlock(t, bm4, bm1block, seededRandomData(31, 100))
	verifyBlock(t, bm4, bm2block, seededRandomData(32, 100))
	verifyBlock(t, bm4, bm3block, seededRandomData(33, 100))

	if got, want := getIndexCount(data), 4; got != want {
		t.Errorf("unexpected index count before compaction: %v, wanted %v", got, want)
	}

	if err := bm4.CompactIndexes(); err != nil {
		t.Errorf("compaction error: %v", err)
	}
	if got, want := getIndexCount(data), 5; got != want {
		t.Errorf("unexpected index count after partial compaction: %v, wanted %v", got, want)
	}

	// new block manager at this point can see all data.
	bm5 := newTestBlockManager(data, keyTime)
	verifyBlock(t, bm5, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(t, bm5, sharedBlock, seededRandomData(20, 100))
	verifyBlock(t, bm5, bm1block, seededRandomData(31, 100))
	verifyBlock(t, bm5, bm2block, seededRandomData(32, 100))
	verifyBlock(t, bm5, bm3block, seededRandomData(33, 100))
	if err := bm5.CompactIndexes(); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func TestDeleteBlock(t *testing.T) {
	data := map[string][]byte{}
	keyTime := map[string]time.Time{}
	bm := newTestBlockManager(data, keyTime)
	setFakeTimeWithAutoAdvance(bm, fakeTime, 1)
	block1 := writeBlockAndVerify(t, bm, "some-group", seededRandomData(10, 100))
	bm.Flush()
	block2 := writeBlockAndVerify(t, bm, "some-group", seededRandomData(11, 100))
	if err := bm.DeleteBlock(block1); err != nil {
		t.Errorf("unable to delete block: %v", block1)
	}
	if err := bm.DeleteBlock(block2); err != nil {
		t.Errorf("unable to delete block: %v", block1)
	}
	verifyBlockNotFound(t, bm, block1)
	verifyBlockNotFound(t, bm, block2)
	bm.Flush()
	bm = newTestBlockManager(data, keyTime)
	dumpBlockManagerData(data)
	verifyBlockNotFound(t, bm, block1)
	verifyBlockNotFound(t, bm, block2)
}

func TestDeleteAndRecreate(t *testing.T) {
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
			bm := newTestBlockManager(data, keyTime)
			setFakeTimeWithAutoAdvance(bm, fakeTime, 1)
			block1 := writeBlockAndVerify(t, bm, "some-group", seededRandomData(10, 100))
			bm.Flush()

			// delete but at given timestamp but don't commit yet.
			bm0 := newTestBlockManager(data, keyTime)
			setFakeTimeWithAutoAdvance(bm0, tc.deletionTime, 1)
			bm0.DeleteBlock(block1)

			// delete it at t0+10
			bm1 := newTestBlockManager(data, keyTime)
			setFakeTimeWithAutoAdvance(bm1, fakeTime.Add(10), 1)
			verifyBlock(t, bm1, block1, seededRandomData(10, 100))
			bm1.DeleteBlock(block1)
			bm1.Flush()

			// recreate at t0+20
			bm2 := newTestBlockManager(data, keyTime)
			setFakeTimeWithAutoAdvance(bm2, fakeTime.Add(20), 1)
			block2 := writeBlockAndVerify(t, bm2, "some-group", seededRandomData(10, 100))
			bm2.Flush()

			// commit deletion from bm0 (t0+5)
			bm0.Flush()

			if block1 != block2 {
				t.Errorf("got invalid block %v, expected %v", block2, block1)
			}

			bm3 := newTestBlockManager(data, keyTime)
			if tc.isVisible {
				verifyBlock(t, bm3, block1, seededRandomData(10, 100))
			} else {
				verifyBlockNotFound(t, bm3, block1)
			}
		})
	}
}

func newTestBlockManager(data map[string][]byte, keyTime map[string]time.Time) *Manager {
	st := storagetesting.NewMapStorage(data, keyTime)
	//st = logging.NewWrapper(st)
	bm, err := NewManager(st, FormattingOptions{
		BlockFormat:            "TESTONLY_MD5",
		MaxPackedContentLength: maxPackedContentLength,
		MaxPackSize:            maxPackSize,
	}, CachingOptions{})
	if err != nil {
		panic("can't create block manager: " + err.Error())
	}

	setFakeTime(bm, fakeTime)
	return bm
}

func getIndexCount(d map[string][]byte) int {
	var cnt int

	for k := range d {
		if strings.HasPrefix(k, packBlockPrefix) {
			cnt++
		}
	}

	return cnt
}

func setFakeTime(bm *Manager, t time.Time) {
	bm.timeNow = func() time.Time { return t }
}

func setFakeTimeWithAutoAdvance(bm *Manager, t time.Time, dt time.Duration) {
	bm.timeNow = func() time.Time {
		ret := t
		t = t.Add(dt)
		return ret
	}
}

func verifyBlockNotFound(t *testing.T, bm *Manager, blockID string) {
	t.Helper()

	b, err := bm.GetBlock(blockID)
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected response from GetBlock(%q), got %v,%v, expected %v", blockID, b, err, storage.ErrBlockNotFound)
	}
}

func verifyBlock(t *testing.T, bm *Manager, blockID string, b []byte) {
	t.Helper()

	b2, err := bm.GetBlock(blockID)
	if err != nil {
		t.Errorf("unable to read block %q: %v", blockID, err)
		return
	}

	if got, want := b2, b; !reflect.DeepEqual(got, want) {
		t.Errorf("block %q data mismatch: got %x (nil:%v), wanted %x (nil:%v)", blockID, got, got == nil, want, want == nil)
	}

	bi, err := bm.BlockInfo(blockID)
	if err != nil {
		t.Errorf("error getting block info %q: %v", blockID, err)
	}

	if got, want := bi.Length, int64(len(b)); got != want {
		t.Errorf("invalid block size for %q: %v, wanted %v", blockID, got, want)
	}

}
func writeBlockAndVerify(t *testing.T, bm *Manager, packGroup string, b []byte) string {
	t.Helper()

	blockID, err := bm.WriteBlock(packGroup, b)
	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := blockID, md5hash(b); got != want {
		t.Errorf("invalid block ID for %x, got %v, want %v", b, got, want)
	}

	verifyBlock(t, bm, blockID, b)

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
		if k[0] == 'P' {
			var payload blockmgrpb.Indexes
			proto.Unmarshal(v, &payload)
			log.Printf("data[%v] = %v", k, proto.MarshalTextString(&payload))
		} else {
			log.Printf("data[%v] = %v bytes", k, len(v))
		}
	}
}

func verifyGroupListContains(t *testing.T, bm *Manager, groupID string, expected ...string) {
	got := map[string]bool{}
	want := map[string]bool{}
	for _, a := range bm.ListGroupBlocks(groupID) {
		got[a.BlockID] = true
	}

	for _, e := range expected {
		want[e] = true
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected contents of group %q: %v, wanted %v", groupID, got, want)
	}
}
