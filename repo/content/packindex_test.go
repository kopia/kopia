package content

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/kopia/kopia/repo/blob"
)

const fakeEncryptionOverhead = 27

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

//nolint:gocyclo
func TestPackIndex(t *testing.T) {
	var infos []Info

	// deleted contents with all information
	for i := 0; i < 100; i++ {
		infos = append(infos, &InfoStruct{
			TimestampSeconds: randomUnixTime(),
			Deleted:          true,
			ContentID:        deterministicContentID("deleted-packed", i),
			PackBlobID:       deterministicPackBlobID(i),
			PackOffset:       deterministicPackedOffset(i),
			PackedLength:     deterministicPackedLength(i),
			FormatVersion:    deterministicFormatVersion(i),
		})
	}
	// non-deleted content
	for i := 0; i < 100; i++ {
		infos = append(infos, &InfoStruct{
			TimestampSeconds: randomUnixTime(),
			ContentID:        deterministicContentID("packed", i),
			PackBlobID:       deterministicPackBlobID(i),
			PackOffset:       deterministicPackedOffset(i),
			PackedLength:     deterministicPackedLength(i),
			FormatVersion:    deterministicFormatVersion(i),
		})
	}

	infoMap := map[ID]Info{}
	b1 := make(packIndexBuilder)
	b2 := make(packIndexBuilder)
	b3 := make(packIndexBuilder)

	for _, info := range infos {
		infoMap[info.GetContentID()] = info
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

	// each build produces exactly idendical prefix except for the trailing random bytes.
	data1Prefix := data1[0 : len(data1)-randomSuffixSize]
	data2Prefix := data2[0 : len(data2)-randomSuffixSize]
	data3Prefix := data3[0 : len(data3)-randomSuffixSize]

	if !bytes.Equal(data1Prefix, data2Prefix) {
		t.Errorf("builder output not stable: %x vs %x", hex.Dump(data1Prefix), hex.Dump(data2Prefix))
	}

	if !bytes.Equal(data2Prefix, data3Prefix) {
		t.Errorf("builder output not stable: %x vs %x", hex.Dump(data2Prefix), hex.Dump(data3Prefix))
	}

	if bytes.Equal(data1, data2) {
		t.Errorf("builder output expected to be different, but was the same")
	}

	t.Run("FuzzTest", func(t *testing.T) {
		fuzzTestIndexOpen(data1)
	})

	ndx, err := openPackIndex(bytes.NewReader(data1), fakeEncryptionOverhead)
	if err != nil {
		t.Fatalf("can't open index: %v", err)
	}
	defer ndx.Close()

	for _, want := range infos {
		info2, err := ndx.GetInfo(want.GetContentID())
		if err != nil {
			t.Errorf("unable to find %v", want.GetContentID())
			continue
		}

		want = withOriginalLength{want, want.GetPackedLength() - fakeEncryptionOverhead}

		if diff := infoDiff(want, info2); len(diff) != 0 {
			t.Errorf("invalid value retrieved: diff: %v", diff)
		}
	}

	cnt := 0

	assertNoError(t, ndx.Iterate(AllIDs, func(info2 Info) error {
		want := infoMap[info2.GetContentID()]
		want = withOriginalLength{want, want.GetPackedLength() - fakeEncryptionOverhead}

		if diff := infoDiff(want, info2); len(diff) != 0 {
			t.Errorf("invalid value retrieved: %v", diff)
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
		assertNoError(t, ndx.Iterate(PrefixRange(prefix), func(info2 Info) error {
			cnt2++
			if !strings.HasPrefix(string(info2.GetContentID()), string(prefix)) {
				t.Errorf("unexpected item %v when iterating prefix %v", info2.GetContentID(), prefix)
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
		ndx, err := openPackIndex(bytes.NewReader(d), 0)
		if err != nil {
			return
		}
		defer ndx.Close()
		cnt := 0
		_ = ndx.Iterate(AllIDs, func(cb Info) error {
			if cnt < 10 {
				_, _ = ndx.GetInfo(cb.GetContentID())
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

type withOriginalLength struct {
	Info
	originalLength uint32
}

func (o withOriginalLength) GetOriginalLength() uint32 {
	return o.originalLength
}

type withDeleted struct {
	Info
	deleted bool
}

func (o withDeleted) GetDeleted() bool {
	return o.deleted
}

func infoDiff(i1, i2 Info, ignore ...string) []string {
	var diffs []string

	if l, r := i1.GetContentID(), i2.GetContentID(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetContentID %v != %v", l, r))
	}

	if l, r := i1.GetPackBlobID(), i2.GetPackBlobID(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackBlobID %v != %v", l, r))
	}

	if l, r := i1.GetDeleted(), i2.GetDeleted(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetDeleted %v != %v", l, r))
	}

	if l, r := i1.GetFormatVersion(), i2.GetFormatVersion(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetFormatVersion %v != %v", l, r))
	}

	if l, r := i1.GetOriginalLength(), i2.GetOriginalLength(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetOriginalLength %v != %v", l, r))
	}

	if l, r := i1.GetPackOffset(), i2.GetPackOffset(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackOffset %v != %v", l, r))
	}

	if l, r := i1.GetPackedLength(), i2.GetPackedLength(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetPackedLength %v != %v", l, r))
	}

	if l, r := i1.GetTimestampSeconds(), i2.GetTimestampSeconds(); l != r {
		diffs = append(diffs, fmt.Sprintf("GetTimestampSeconds %v != %v", l, r))
	}

	var result []string

	for _, v := range diffs {
		ignored := false

		for _, ign := range ignore {
			if strings.HasPrefix(v, ign) {
				ignored = true
			}
		}

		if !ignored {
			result = append(result, v)
		}
	}

	return result
}
