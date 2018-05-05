package packindex_test

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/kopia/kopia/internal/packindex"
)

func TestPackIndex(t *testing.T) {
	b := packindex.NewBuilder()

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
	deterministicPackBlockID := func(id int) packindex.PhysicalBlockID {
		h := sha1.New()
		fmt.Fprintf(h, "%v", id)
		blockNumber++
		return packindex.PhysicalBlockID(fmt.Sprintf("%x", h.Sum(nil)))
	}
	deterministicPayload := func(id int) []byte {
		s := rand.NewSource(int64(id))
		rnd := rand.New(s)
		length := rnd.Intn(1000)
		payload := make([]byte, length)
		rnd.Read(payload)
		return payload
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
	for i := 0; i < 100; i++ {
		infos = append(infos, packindex.Info{
			BlockID:          deterministicBlockID("del", i),
			TimestampSeconds: randomUnixTime(),
			Deleted:          true,
		})
	}
	for i := 0; i < 100; i++ {
		p := deterministicPayload(i)
		infos = append(infos, packindex.Info{
			BlockID:          deterministicBlockID("inline", i),
			TimestampSeconds: randomUnixTime(),
			Length:           uint32(len(p)),
			Payload:          p,
		})
	}
	for i := 0; i < 100; i++ {
		infos = append(infos, packindex.Info{
			TimestampSeconds: randomUnixTime(),
			BlockID:          deterministicBlockID("packed", i),
			PackBlockID:      deterministicPackBlockID(i),
			PackOffset:       deterministicPackedOffset(i),
			Length:           deterministicPackedLength(i),
			FormatVersion:    deterministicFormatVersion(i),
		})
	}

	infoMap := map[string]packindex.Info{}

	for _, info := range infos {
		infoMap[info.BlockID] = info
		b.Add(info)
	}

	var buf bytes.Buffer
	if err := b.Build(&buf); err != nil {
		t.Errorf("unable to build: %v", err)
	}

	data := buf.Bytes()
	ndx, err := packindex.Open(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("can't open index: %v", err)
	}
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
