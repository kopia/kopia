package block

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/storage"

	"github.com/kopia/kopia/internal/storagetesting"
)

const (
	maxPackedContentLength = 1000
	maxPackSize            = 2000
)

var fakeTime = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)

func TestBlockManagerEmptyFlush(t *testing.T) {
	data := map[string][]byte{}
	bm := newTestBlockManager(data)
	bm.Flush()
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
}

func TestBlockZeroBytes1(t *testing.T) {
	data := map[string][]byte{}
	bm := newTestBlockManager(data)
	writeBlockAndVerify(t, bm, "", []byte{})
	bm.Flush()
	if got, want := len(data), 2; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
	}
	dumpBlockManagerData(data)
}

func TestBlockZeroBytes2(t *testing.T) {
	data := map[string][]byte{}
	bm := newTestBlockManager(data)
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
	bm := newTestBlockManager(data)

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
	bm := newTestBlockManager(data)

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
	bm := newTestBlockManager(data)

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
	bm := newTestBlockManager(data)

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
	bm := newTestBlockManager(data)

	noSuchBlockID := md5hash([]byte("foo"))

	b, err := bm.GetBlock(noSuchBlockID)
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block: %v, %v", b, err)
	}

	bs, err := bm.BlockSize(noSuchBlockID)
	if err != storage.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block size: %v, %v", bs, err)
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
		bm := newTestBlockManager(data)

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

func TestBlockManagerInternalFlush(t *testing.T) {
	data := map[string][]byte{}
	bm := newTestBlockManager(data)

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
	bm := newTestBlockManager(data)

	var blockIDs []string

	for i := 0; i < 5000; i++ {
		//t.Logf("i=%v", i)
		b := seededRandomData(i, i%113)
		//t.Logf("writing block #%v with %x", i, b)
		blkID, err := bm.WriteBlock("", b, "")
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
			bm = newTestBlockManager(data)
		}
	}

	for _, blockID := range blockIDs {
		_, err := bm.GetBlock(blockID)
		if err != nil {
			t.Errorf("can't read block %q: %v", blockID, err)
			continue
		}
	}

	//dumpBlockManagerData(data)
}

func TestBlockManagerConcurrency(t *testing.T) {
	data := map[string][]byte{}
	bm := newTestBlockManager(data)
	preexistingBlock := writeBlockAndVerify(t, bm, "", seededRandomData(10, 100))
	bm.Flush()

	bm1 := newTestBlockManager(data)
	bm2 := newTestBlockManager(data)
	bm3 := newTestBlockManager(data)
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
	bm4 := newTestBlockManager(data)
	verifyBlock(t, bm4, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(t, bm4, sharedBlock, seededRandomData(20, 100))
	verifyBlock(t, bm4, bm1block, seededRandomData(31, 100))
	verifyBlock(t, bm4, bm2block, seededRandomData(32, 100))
	verifyBlock(t, bm4, bm3block, seededRandomData(33, 100))

	if got, want := getIndexCount(data), 4; got != want {
		t.Errorf("unexpected index count before compaction: %v, wanted %v", got, want)
	}

	if err := bm4.CompactIndexes(fakeTime.Add(-1), nil); err != nil {
		t.Errorf("compaction error: %v", err)
	}

	if got, want := getIndexCount(data), 4; got != want {
		t.Errorf("unexpected index count after no-op compaction: %v, wanted %v", got, want)
	}

	if err := bm4.CompactIndexes(fakeTime, nil); err != nil {
		t.Errorf("compaction error: %v", err)
	}
	if got, want := getIndexCount(data), 2; got != want {
		t.Errorf("unexpected index count after partial compaction: %v, wanted %v", got, want)
	}

	if err := bm4.CompactIndexes(fakeTime.Add(1), nil); err != nil {
		t.Errorf("compaction error: %v", err)
	}
	if got, want := getIndexCount(data), 1; got != want {
		t.Errorf("unexpected index count after full compaction: %v, wanted %v", got, want)
	}

	// new block manager at this point can see all data.
	bm5 := newTestBlockManager(data)
	verifyBlock(t, bm5, preexistingBlock, seededRandomData(10, 100))
	verifyBlock(t, bm5, sharedBlock, seededRandomData(20, 100))
	verifyBlock(t, bm5, bm1block, seededRandomData(31, 100))
	verifyBlock(t, bm5, bm2block, seededRandomData(32, 100))
	verifyBlock(t, bm5, bm3block, seededRandomData(33, 100))
	if err := bm5.CompactIndexes(fakeTime, nil); err != nil {
		t.Errorf("compaction error: %v", err)
	}
}

func newTestBlockManager(data map[string][]byte) *Manager {
	st := storagetesting.NewMapStorage(data)

	f := &unencryptedFormat{computeHash(md5.New, md5.Size)}
	//st = logging.NewWrapper(st)
	bm := NewManager(st, maxPackedContentLength, maxPackSize, f)

	setFakeTime(bm, fakeTime)
	return bm
}

func getIndexCount(d map[string][]byte) int {
	var cnt int

	for k := range d {
		if strings.HasPrefix(k, packObjectPrefix) {
			cnt++
		}
	}

	return cnt
}

func setFakeTime(bm *Manager, t time.Time) {
	bm.timeNow = func() time.Time { return t }
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
	}

	if got, want := b2, b; !reflect.DeepEqual(got, want) {
		t.Errorf("block %q data mismatch: got %x (nil:%v), wanted %x (nil:%v)", blockID, got, got == nil, want, want == nil)
	}

	bs, err := bm.BlockSize(blockID)
	if err != nil {
		t.Errorf("error getting block size %q: %v", blockID, err)
	}

	if got, want := bs, int64(len(b)); got != want {
		t.Errorf("invalid block size for %q: %v, wanted %v", blockID, got, want)
	}

}
func writeBlockAndVerify(t *testing.T, bm *Manager, packGroup string, b []byte) string {
	t.Helper()

	blockID, err := bm.WriteBlock(packGroup, b, "")
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
			gz, _ := gzip.NewReader(bytes.NewReader(v))
			var buf bytes.Buffer
			buf.ReadFrom(gz)

			var dst bytes.Buffer
			json.Indent(&dst, buf.Bytes(), "", "  ")

			log.Printf("data[%v] = %v", k, dst.String())
		} else {
			log.Printf("data[%v] = %x", k, v)
		}
	}
}
