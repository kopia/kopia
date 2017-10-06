package repo

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"log"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/blob/logging"

	"github.com/kopia/kopia/internal/storagetesting"
)

const (
	maxPackedContentLength = 1000
	maxPackSize            = 2000
)

func TestBlockManagerEmptyFlush(t *testing.T) {
	data := map[string][]byte{}
	bm := newTestBlockManager(data)
	bm.Flush()
	if got, want := len(data), 0; got != want {
		t.Errorf("unexpected number of blocks: %v, wanted %v", got, want)
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
	log.Printf("finished writing again")
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
	if err != blob.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block: %v, %v", b, err)
	}

	bs, err := bm.BlockSize(noSuchBlockID)
	if err != blob.ErrBlockNotFound {
		t.Errorf("unexpected error when getting non-existent block size: %v, %v", bs, err)
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
}
func newTestBlockManager(data map[string][]byte) *blockManager {
	s := storagetesting.NewMapStorage(data)

	f := &unencryptedFormat{computeHash(md5.New, md5.Size)}
	bm := newBlockManager(logging.NewWrapper(s), maxPackedContentLength, maxPackSize, f)

	bm.timeNow = func() time.Time { return time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC) }
	return bm
}

func writeBlockAndVerify(t *testing.T, bm *blockManager, packGroup string, b []byte) string {
	t.Helper()

	blockID, err := bm.WriteBlock(packGroup, b, "")
	if err != nil {
		t.Errorf("err: %v", err)
	}

	if got, want := blockID, md5hash(b); got != want {
		t.Errorf("invalid block ID for %x, got %v, want %v", b, got, want)
	}

	b2, err := bm.GetBlock(blockID)
	if err != nil {
		t.Errorf("unable to read block %q that was just written: %v", blockID, err)
	}

	if got, want := b2, b; !reflect.DeepEqual(got, want) {
		t.Errorf("block %q data mismatch: got %x, wanted %x", blockID, got, want)
	}

	bs, err := bm.BlockSize(blockID)
	if err != nil {
		t.Errorf("error getting block size %q: %v", blockID, err)
	}

	if got, want := bs, int64(len(b)); got != want {
		t.Errorf("invalid block size for %q: %v, wanted %v", blockID, got, want)
	}

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

			log.Printf("data[%v] = %v", k, buf.String())
		} else {
			log.Printf("data[%v] = %x", k, v)
		}
	}
}
