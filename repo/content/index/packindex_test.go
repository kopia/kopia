package index

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

const fakeEncryptionOverhead = 27

func deterministicContentID(t *testing.T, prefix string, id int) ID {
	t.Helper()

	h := sha1.New()
	fmt.Fprintf(h, "%v%v", prefix, id)

	var prefix2 IDPrefix
	if id%2 == 0 {
		prefix2 = "x"
	}

	if id%7 == 0 {
		prefix2 = "y"
	}

	if id%5 == 0 {
		prefix2 = "m"
	}

	cid, err := IDFromHash(prefix2, h.Sum(nil))
	require.NoError(t, err)

	return cid
}

func deterministicPackBlobID(id int) blob.ID {
	h := sha1.New()
	fmt.Fprintf(h, "%v", id)

	return blob.ID(hex.EncodeToString(h.Sum(nil)))
}

func deterministicPackedOffset(id int) uint32 {
	s := rand.NewSource(int64(id + 1))
	rnd := rand.New(s)

	return uint32(rnd.Int31()) & (1<<28 - 1)
}

func deterministicOriginalLength(id, version int) uint32 {
	if version == 1 {
		return deterministicPackedLength(id) - fakeEncryptionOverhead
	}

	s := rand.NewSource(int64(id + 4))
	rnd := rand.New(s)

	return uint32(rnd.Int31()) & (1<<28 - 1)
}

func deterministicPackedLength(id int) uint32 {
	s := rand.NewSource(int64(id + 2))
	rnd := rand.New(s)

	return uint32(rnd.Int31()) % v2MaxContentLength
}

func deterministicFormatVersion(id int) byte {
	return byte(id % 100)
}

func deterministicCompressionHeaderID(id, version int) compression.HeaderID {
	if version == 1 {
		return 0
	}

	return compression.HeaderID(id % 100)
}

func deterministicEncryptionKeyID(id, version int) byte {
	if version == 1 {
		return 0
	}

	return byte(id % 100)
}

func randomUnixTime() int64 {
	return int64(rand.Int31())
}

func TestPackIndex_V1(t *testing.T) {
	testPackIndex(t, Version1)
}

func TestPackIndex_V2(t *testing.T) {
	testPackIndex(t, Version2)
}

//nolint:thelper,gocyclo,cyclop
func testPackIndex(t *testing.T, version int) {
	var infos []Info
	// deleted contents with all information
	for i := 0; i < 100; i++ {
		infos = append(infos, Info{
			TimestampSeconds:    randomUnixTime(),
			Deleted:             true,
			ContentID:           deterministicContentID(t, "deleted-packed", i),
			PackBlobID:          deterministicPackBlobID(i),
			PackOffset:          deterministicPackedOffset(i),
			PackedLength:        deterministicPackedLength(i),
			FormatVersion:       deterministicFormatVersion(i),
			OriginalLength:      deterministicOriginalLength(i, version),
			CompressionHeaderID: deterministicCompressionHeaderID(i, version),
			EncryptionKeyID:     deterministicEncryptionKeyID(i, version),
		})
	}
	// non-deleted content
	for i := 0; i < 100; i++ {
		infos = append(infos, Info{
			TimestampSeconds:    randomUnixTime(),
			ContentID:           deterministicContentID(t, "packed", i),
			PackBlobID:          deterministicPackBlobID(i),
			PackOffset:          deterministicPackedOffset(i),
			PackedLength:        deterministicPackedLength(i),
			FormatVersion:       deterministicFormatVersion(i),
			OriginalLength:      deterministicOriginalLength(i, version),
			CompressionHeaderID: deterministicCompressionHeaderID(i, version),
			EncryptionKeyID:     deterministicEncryptionKeyID(i, version),
		})
	}

	// dear future reader, if this fails because the number of methods has changed,
	// you need to add additional test cases above.
	if cnt := reflect.TypeOf((*InfoReader)(nil)).Elem().NumMethod(); cnt != 11 {
		t.Fatalf("unexpected number of methods on content.Info: %v, must update the test", cnt)
	}

	infoMap := map[ID]Info{}
	b1 := make(Builder)
	b2 := make(Builder)
	b3 := make(Builder)

	for _, info := range infos {
		infoMap[info.GetContentID()] = info
		b1.Add(info)
		b2.Add(info)
		b3.Add(info)
	}

	var buf1, buf2, buf3 bytes.Buffer

	if err := b1.Build(&buf1, version); err != nil {
		t.Fatalf("unable to build: %v", err)
	}

	if err := b2.Build(&buf2, version); err != nil {
		t.Fatalf("unable to build: %v", err)
	}

	if err := b3.BuildStable(&buf3, version); err != nil {
		t.Fatalf("unable to build: %v", err)
	}

	data1 := buf1.Bytes()
	data2 := buf2.Bytes()
	data3 := buf3.Bytes()

	// each build produces exactly identical prefix except for the trailing random bytes.
	data1Prefix := data1[0 : len(data1)-randomSuffixSize]
	data2Prefix := data2[0 : len(data2)-randomSuffixSize]

	require.Equal(t, data1Prefix, data2Prefix)
	require.Equal(t, data2Prefix, data3)
	require.NotEqual(t, data1, data2)

	t.Run("FuzzTest", func(t *testing.T) {
		fuzzTestIndexOpen(data1)
	})

	ndx, err := Open(data1, nil, func() int { return fakeEncryptionOverhead })
	if err != nil {
		t.Fatalf("can't open index: %v", err)
	}

	for _, want := range infos {
		info2, err := ndx.GetInfo(want.GetContentID())
		if err != nil {
			t.Errorf("unable to find %v", want.GetContentID())
			continue
		}

		if version == 1 {
			// v1 does not preserve original length.
			want = withOriginalLength(want, want.GetPackedLength()-fakeEncryptionOverhead)
		}

		require.Equal(t, want, ToInfoStruct(info2))
	}

	cnt := 0

	require.NoError(t, ndx.Iterate(AllIDs, func(info2 InfoReader) error {
		want := infoMap[info2.GetContentID()]
		if version == 1 {
			// v1 does not preserve original length.
			want = withOriginalLength(want, want.GetPackedLength()-fakeEncryptionOverhead)
		}

		require.Equal(t, want, ToInfoStruct(info2))
		cnt++
		return nil
	}))

	if cnt != len(infoMap) {
		t.Errorf("invalid number of iterations: %v, wanted %v", cnt, len(infoMap))
	}

	prefixes := []IDPrefix{"a", "b", "f", "0", "3", "aa", "aaa", "aab", "fff", "m", "x", "y", "m0", "ma"}

	for i := 0; i < 100; i++ {
		contentID := deterministicContentID(t, "no-such-content", i)

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
		require.NoError(t, ndx.Iterate(PrefixRange(prefix), func(info2 InfoReader) error {
			cnt2++
			if !strings.HasPrefix(info2.GetContentID().String(), string(prefix)) {
				t.Errorf("unexpected item %v when iterating prefix %v", info2.GetContentID(), prefix)
			}
			return nil
		}))
		t.Logf("found %v elements with prefix %q", cnt2, prefix)
	}
}

func TestPackIndexPerContentLimits(t *testing.T) {
	cases := []struct {
		info   Info
		errMsg string
	}{
		{Info{PackedLength: v2MaxContentLength}, "maximum content length is too high"},
		{Info{PackedLength: v2MaxContentLength - 1}, ""},
		{Info{OriginalLength: v2MaxContentLength}, "maximum content length is too high"},
		{Info{OriginalLength: v2MaxContentLength - 1}, ""},
		{Info{PackOffset: v2MaxPackOffset}, "pack offset 1073741824 is too high"},
		{Info{PackOffset: v2MaxPackOffset - 1}, ""},
	}

	for _, tc := range cases {
		cid := deterministicContentID(t, "hello-world", 1)
		tc.info.ContentID = cid

		b := Builder{
			cid: tc.info,
		}

		var result bytes.Buffer

		if tc.errMsg == "" {
			require.NoError(t, b.buildV2(&result))

			pi, err := Open(result.Bytes(), nil, func() int { return fakeEncryptionOverhead })
			require.NoError(t, err)

			got, err := pi.GetInfo(cid)
			require.NoError(t, err)

			require.Equal(t, ToInfoStruct(got), tc.info)
		} else {
			err := b.buildV2(&result)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		}
	}
}

func TestSortedContents(t *testing.T) {
	b := Builder{}

	for i := 0; i < 100; i++ {
		v := deterministicContentID(t, "", i)

		b.Add(Info{
			ContentID: v,
		})
	}

	got := b.sortedContents()

	var last ID
	for _, info := range got {
		if info.GetContentID().less(last) {
			t.Fatalf("not sorted %v (was %v)!", info.GetContentID(), last)
		}

		last = info.GetContentID()
	}
}

func TestSortedContents2(t *testing.T) {
	b := Builder{}

	b.Add(Info{
		ContentID: mustParseID(t, "0123"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "1023"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "0f23"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "f023"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "g0123"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "g1023"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "i0123"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "i1023"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "h0123"),
	})
	b.Add(Info{
		ContentID: mustParseID(t, "h1023"),
	})

	got := b.sortedContents()

	var last ID

	for _, info := range got {
		if info.GetContentID().less(last) {
			t.Fatalf("not sorted %v (was %v)!", info.GetContentID(), last)
		}

		last = info.GetContentID()
	}
}

func TestPackIndexV2TooManyUniqueFormats(t *testing.T) {
	b := Builder{}

	for i := 0; i < v2MaxFormatCount; i++ {
		v := deterministicContentID(t, "", i)

		b.Add(Info{
			ContentID:           v,
			PackBlobID:          blob.ID(v.String()),
			FormatVersion:       1,
			CompressionHeaderID: compression.HeaderID(1000 + i),
		})
	}

	require.NoError(t, b.buildV2(io.Discard))

	// add one more to push it over the edge
	b.Add(Info{
		ContentID:           deterministicContentID(t, "", v2MaxFormatCount),
		FormatVersion:       1,
		CompressionHeaderID: compression.HeaderID(5000),
	})

	err := b.buildV2(io.Discard)
	require.Error(t, err)
	require.Equal(t, "unsupported - too many unique formats 256 (max 255)", err.Error())
}

func fuzzTestIndexOpen(originalData []byte) {
	// use consistent random
	rnd := rand.New(rand.NewSource(12345))

	fuzzTest(rnd, originalData, 50000, func(d []byte) {
		ndx, err := Open(d, nil, func() int { return 0 })
		if err != nil {
			return
		}
		cnt := 0
		_ = ndx.Iterate(AllIDs, func(cb InfoReader) error {
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

func TestShard(t *testing.T) {
	b := Builder{}

	// generate 10000 IDs in random order
	ids := make([]int, 10000)
	for i := range ids {
		ids[i] = i
	}

	rand.Shuffle(len(ids), func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})

	// add ID to the builder
	for _, id := range ids {
		b.Add(Info{
			ContentID: deterministicContentID(t, "", id),
		})
	}

	// verify number of shards
	verifyAllShardedIDs(t, b.shard(100000), len(b), 1)
	verifyAllShardedIDs(t, b.shard(100), len(b), 100)

	// sharding will always produce stable results, verify sorted shard lengths here
	require.ElementsMatch(t,
		[]int{460, 472, 473, 477, 479, 483, 486, 492, 498, 499, 501, 503, 504, 505, 511, 519, 524, 528, 542, 544},
		verifyAllShardedIDs(t, b.shard(500), len(b), 20))
	require.ElementsMatch(t,
		[]int{945, 964, 988, 988, 993, 1002, 1014, 1017, 1021, 1068},
		verifyAllShardedIDs(t, b.shard(1000), len(b), 10))
	require.ElementsMatch(t,
		[]int{1952, 1995, 2005, 2013, 2035},
		verifyAllShardedIDs(t, b.shard(2000), len(b), 5))
}

func verifyAllShardedIDs(t *testing.T, sharded []Builder, numTotal, numShards int) []int {
	t.Helper()

	require.Len(t, sharded, numShards)

	m := map[ID]bool{}
	for i := 0; i < numTotal; i++ {
		m[deterministicContentID(t, "", i)] = true
	}

	cnt := 0

	var lens []int

	for _, s := range sharded {
		cnt += len(s)
		lens = append(lens, len(s))

		for _, v := range s {
			delete(m, v.GetContentID())
		}
	}

	require.Equal(t, numTotal, cnt, "invalid total number of sharded elements")
	require.Empty(t, m)

	return lens
}

func withOriginalLength(is Info, originalLength uint32) Info {
	// clone and override original length
	is.OriginalLength = originalLength

	return is
}

func mustParseID(t *testing.T, s string) ID {
	t.Helper()

	id, err := ParseID(s)
	require.NoError(t, err)

	return id
}
