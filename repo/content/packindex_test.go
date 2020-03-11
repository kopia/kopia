package content

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/kopia/kopia/repo/blob"
)

func deterministicContentID(prefix string, id int) ID {
	h := sha1.New()
	fmt.Fprintf(h, "%v%v", prefix, id)

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

	return ID(fmt.Sprintf("%v%x", prefix2, h.Sum(nil)))
}
func deterministicPackBlobID(id int) blob.ID {
	h := sha1.New()
	fmt.Fprintf(h, "%v", id)

	return blob.ID(fmt.Sprintf("%x", h.Sum(nil)))
}

func deterministicPackedOffset(id int) uint32 {
	s := rand.NewSource(int64(id + 1))
	rnd := rand.New(s)

	return uint32(rnd.Int31())
}
func deterministicPackedLength(id int) uint32 {
	s := rand.NewSource(int64(id + 2))
	rnd := rand.New(s)

	return uint32(rnd.Int31())
}
func deterministicFormatVersion(id int) byte {
	return byte(id % 100)
}

func randomUnixTime() int64 {
	return int64(rand.Int31())
}

//nolint:gocyclo,funlen
func TestPackIndex(t *testing.T) {
	var infos []Info

	// deleted contents with all information
	for i := 0; i < 100; i++ {
		infos = append(infos, Info{
			TimestampSeconds: randomUnixTime(),
			Deleted:          true,
			ID:               deterministicContentID("deleted-packed", i),
			PackBlobID:       deterministicPackBlobID(i),
			PackOffset:       deterministicPackedOffset(i),
			Length:           deterministicPackedLength(i),
			FormatVersion:    deterministicFormatVersion(i),
		})
	}
	// non-deleted content
	for i := 0; i < 100; i++ {
		infos = append(infos, Info{
			TimestampSeconds: randomUnixTime(),
			ID:               deterministicContentID("packed", i),
			PackBlobID:       deterministicPackBlobID(i),
			PackOffset:       deterministicPackedOffset(i),
			Length:           deterministicPackedLength(i),
			FormatVersion:    deterministicFormatVersion(i),
		})
	}

	infoMap := map[ID]Info{}
	b1 := make(packIndexBuilder)
	b2 := make(packIndexBuilder)
	b3 := make(packIndexBuilder)

	for _, info := range infos {
		infoMap[info.ID] = info
		b1.Add(info)
		b2.Add(info)
		b3.Add(info)
	}

	var buf1, buf2, buf3 bytes.Buffer

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

	if !bytes.Equal(data1, data2) {
		t.Errorf("builder output not stable: %x vs %x", hex.Dump(data1), hex.Dump(data2))
	}

	if !bytes.Equal(data2, data3) {
		t.Errorf("builder output not stable: %x vs %x", hex.Dump(data2), hex.Dump(data3))
	}

	t.Run("FuzzTest", func(t *testing.T) {
		fuzzTestIndexOpen(data1)
	})

	ndx, err := openPackIndex(bytes.NewReader(data1))
	if err != nil {
		t.Fatalf("can't open index: %v", err)
	}
	defer ndx.Close()

	for _, info := range infos {
		info2, err := ndx.GetInfo(info.ID)
		if err != nil {
			t.Errorf("unable to find %v", info.ID)
			continue
		}

		if !reflect.DeepEqual(info, *info2) {
			t.Errorf("invalid value retrieved: %+v, wanted %+v", info2, info)
		}
	}

	cnt := 0

	assertNoError(t, ndx.Iterate("", func(info2 Info) error {
		info := infoMap[info2.ID]
		if !reflect.DeepEqual(info, info2) {
			t.Errorf("invalid value retrieved: %+v, wanted %+v", info2, info)
		}
		cnt++
		return nil
	}))

	if cnt != len(infoMap) {
		t.Errorf("invalid number of iterations: %v, wanted %v", cnt, len(infoMap))
	}

	prefixes := []ID{"a", "b", "f", "0", "3", "aa", "aaa", "aab", "fff", "m", "x", "y", "m0", "ma"}

	for i := 0; i < 100; i++ {
		contentID := deterministicContentID("no-such-content", i)

		v, err := ndx.GetInfo(contentID)
		if err != nil {
			t.Errorf("unable to get content %v: %v", contentID, err)
		}

		if v != nil {
			t.Errorf("unexpected result when getting content %v: %v", contentID, v)
		}
	}

	for _, prefix := range prefixes {
		cnt2 := 0
		prefix := prefix
		assertNoError(t, ndx.Iterate(prefix, func(info2 Info) error {
			cnt2++
			if !strings.HasPrefix(string(info2.ID), string(prefix)) {
				t.Errorf("unexpected item %v when iterating prefix %v", info2.ID, prefix)
			}
			return nil
		}))
		t.Logf("found %v elements with prefix %q", cnt2, prefix)
	}
}

func fuzzTestIndexOpen(originalData []byte) {
	// use consistent random
	rnd := rand.New(rand.NewSource(12345))

	fuzzTest(rnd, originalData, 50000, func(d []byte) {
		ndx, err := openPackIndex(bytes.NewReader(d))
		if err != nil {
			return
		}
		defer ndx.Close()
		cnt := 0
		_ = ndx.Iterate("", func(cb Info) error {
			if cnt < 10 {
				_, _ = ndx.GetInfo(cb.ID)
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
