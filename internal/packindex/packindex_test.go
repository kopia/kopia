package packindex_test

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/kopia/kopia/internal/packindex"
)

func TestPackIndex(t *testing.T) {
	blockNumber := 0

	deterministicBlockID := func(prefix string, id int) string {
		h := sha1.New()
		fmt.Fprintf(h, "%v%v", prefix, id)
		blockNumber++

		prefix2 := ""
		if id%2 == 0 {
			prefix2 = "x"
		}
		if id%7 == 0 {
			prefix2 = "y"
		}
		if id%5 == 0 {
			prefix2 = "m"
		}
		return string(fmt.Sprintf("%v%x", prefix2, h.Sum(nil)))
	}
	deterministicPackFile := func(id int) string {
		h := sha1.New()
		fmt.Fprintf(h, "%v", id)
		blockNumber++
		return string(fmt.Sprintf("%x", h.Sum(nil)))
	}

	deterministicPackedOffset := func(id int) uint32 {
		s := rand.NewSource(int64(id + 1))
		rnd := rand.New(s)
		return uint32(rnd.Int31())
	}
	deterministicPackedLength := func(id int) uint32 {
		s := rand.NewSource(int64(id + 2))
		rnd := rand.New(s)
		return uint32(rnd.Int31())
	}
	deterministicFormatVersion := func(id int) byte {
		return byte(id % 100)
	}

	randomUnixTime := func() int64 {
		return int64(rand.Int31())
	}

	var infos []packindex.Info

	// deleted blocks with all information
	for i := 0; i < 100; i++ {
		infos = append(infos, packindex.Info{
			TimestampSeconds: randomUnixTime(),
			Deleted:          true,
			BlockID:          deterministicBlockID("deleted-packed", i),
			PackFile:         deterministicPackFile(i),
			PackOffset:       deterministicPackedOffset(i),
			Length:           deterministicPackedLength(i),
			FormatVersion:    deterministicFormatVersion(i),
		})
	}
	// non-deleted block
	for i := 0; i < 100; i++ {
		infos = append(infos, packindex.Info{
			TimestampSeconds: randomUnixTime(),
			BlockID:          deterministicBlockID("packed", i),
			PackFile:         deterministicPackFile(i),
			PackOffset:       deterministicPackedOffset(i),
			Length:           deterministicPackedLength(i),
			FormatVersion:    deterministicFormatVersion(i),
		})
	}

	infoMap := map[string]packindex.Info{}
	b1 := packindex.NewBuilder()
	b2 := packindex.NewBuilder()
	b3 := packindex.NewBuilder()

	for _, info := range infos {
		infoMap[info.BlockID] = info
		b1.Add(info)
		b2.Add(info)
		b3.Add(info)
	}

	var buf1 bytes.Buffer
	var buf2 bytes.Buffer
	var buf3 bytes.Buffer
	if err := b1.Build(&buf1); err != nil {
		t.Errorf("unable to build: %v", err)
	}
	if err := b1.Build(&buf2); err != nil {
		t.Errorf("unable to build: %v", err)
	}
	if err := b1.Build(&buf3); err != nil {
		t.Errorf("unable to build: %v", err)
	}
	data1 := buf1.Bytes()
	data2 := buf2.Bytes()
	data3 := buf3.Bytes()

	if !reflect.DeepEqual(data1, data2) {
		t.Errorf("builder output not stable: %x vs %x", hex.Dump(data1), hex.Dump(data2))
	}
	if !reflect.DeepEqual(data2, data3) {
		t.Errorf("builder output not stable: %x vs %x", hex.Dump(data2), hex.Dump(data3))
	}

	t.Run("FuzzTest", func(t *testing.T) {
		fuzzTestIndexOpen(t, data1)
	})

	ndx, err := packindex.Open(bytes.NewReader(data1))
	if err != nil {
		t.Fatalf("can't open index: %v", err)
	}
	defer ndx.Close()

	for _, info := range infos {
		info2, err := ndx.GetInfo(info.BlockID)
		if err != nil {
			t.Errorf("unable to find %v", info.BlockID)
			continue
		}
		if !reflect.DeepEqual(info, *info2) {
			t.Errorf("invalid value retrieved: %+v, wanted %+v", info2, info)
		}
	}

	cnt := 0
	ndx.Iterate("", func(info2 packindex.Info) error {
		info := infoMap[info2.BlockID]
		if !reflect.DeepEqual(info, info2) {
			t.Errorf("invalid value retrieved: %+v, wanted %+v", info2, info)
		}
		cnt++
		return nil
	})
	if cnt != len(infoMap) {
		t.Errorf("invalid number of iterations: %v, wanted %v", cnt, len(infoMap))
	}

	prefixes := []string{"a", "b", "f", "0", "3", "aa", "aaa", "aab", "fff", "m", "x", "y", "m0", "ma"}

	for i := 0; i < 100; i++ {
		blockID := deterministicBlockID("no-such-block", i)
		v, err := ndx.GetInfo(blockID)
		if err != nil {
			t.Errorf("unable to get block %v: %v", blockID, err)
		}
		if v != nil {
			t.Errorf("unexpected result when getting block %v: %v", blockID, v)
		}
	}

	for _, prefix := range prefixes {
		cnt2 := 0
		ndx.Iterate(string(prefix), func(info2 packindex.Info) error {
			cnt2++
			if !strings.HasPrefix(string(info2.BlockID), string(prefix)) {
				t.Errorf("unexpected item %v when iterating prefix %v", info2.BlockID, prefix)
			}
			return nil
		})
		t.Logf("found %v elements with prefix %q", cnt2, prefix)
	}
}

func fuzzTestIndexOpen(t *testing.T, originalData []byte) {
	// use consistent random
	rnd := rand.New(rand.NewSource(12345))

	fuzzTest(rnd, originalData, 50000, func(d []byte) {
		ndx, err := packindex.Open(bytes.NewReader(d))
		if err != nil {
			return
		}
		defer ndx.Close()
		cnt := 0
		ndx.Iterate("", func(cb packindex.Info) error {
			if cnt < 10 {
				ndx.GetInfo(cb.BlockID)
			}
			cnt++
			return nil
		})
	})
}

func fuzzTest(rnd *rand.Rand, originalData []byte, rounds int, callback func(d []byte)) {
	for round := 0; round < rounds; round++ {
		data := append([]byte(nil), originalData...)

		// mutate small number of bytes
		bytesToMutate := rnd.Intn(3)
		for i := 0; i < bytesToMutate; i++ {
			pos := rnd.Intn(len(data))
			data[pos] = byte(rnd.Int())
		}

		sectionsToInsert := rnd.Intn(3)
		for i := 0; i < sectionsToInsert; i++ {
			pos := rnd.Intn(len(data))
			insertedLength := rnd.Intn(20)
			insertedData := make([]byte, insertedLength)
			rnd.Read(insertedData)

			data = append(append(append([]byte(nil), data[0:pos]...), insertedData...), data[pos:]...)
		}

		sectionsToDelete := rnd.Intn(3)
		for i := 0; i < sectionsToDelete; i++ {
			pos := rnd.Intn(len(data))
			deletedLength := rnd.Intn(10)
			if pos+deletedLength > len(data) {
				continue
			}

			data = append(append([]byte(nil), data[0:pos]...), data[pos+deletedLength:]...)
		}

		callback(data)
	}
}
