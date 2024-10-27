package index

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
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
	for i := range 100 {
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
	for i := range 100 {
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

	infoMap := map[ID]Info{}
	b1 := make(Builder)
	b2 := make(Builder)
	b3 := make(Builder)
	b4 := NewOneUseBuilder()

	for _, info := range infos {
		infoMap[info.ContentID] = info
		b1.Add(info)
		b2.Add(info)
		b3.Add(info)
		b4.Add(info)
	}

	var buf1, buf2, buf3, buf4 bytes.Buffer

	err := b1.Build(&buf1, version)
	require.NoError(t, err)

	err = b2.Build(&buf2, version)
	require.NoError(t, err)

	err = b3.BuildStable(&buf3, version)
	require.NoError(t, err)

	err = b4.BuildStable(&buf4, version)
	require.NoError(t, err)

	data1 := buf1.Bytes()
	data2 := buf2.Bytes()
	data3 := buf3.Bytes()
	data4 := buf4.Bytes()

	// each build produces exactly identical prefix except for the trailing random bytes.
	data1Prefix := data1[0 : len(data1)-randomSuffixSize]
	data2Prefix := data2[0 : len(data2)-randomSuffixSize]

	require.Equal(t, data1Prefix, data2Prefix)
	require.Equal(t, data2Prefix, data3)
	require.Equal(t, data2Prefix, data4)
	require.NotEqual(t, data1, data2)
	require.Equal(t, data3, data4)

	t.Run("FuzzTest", func(t *testing.T) {
		fuzzTestIndexOpen(data1)
	})

	verifyPackedIndexes(t, infos, infoMap, version, data1)
	verifyPackedIndexes(t, infos, infoMap, version, data4)
}

func verifyPackedIndexes(t *testing.T, infos []Info, infoMap map[ID]Info, version int, packed []byte) {
	t.Helper()

	ndx, err := Open(packed, nil, func() int { return fakeEncryptionOverhead })
	if err != nil {
		t.Fatalf("can't open index: %v", err)
	}

	for _, want := range infos {
		var info2 Info

		ok, err := ndx.GetInfo(want.ContentID, &info2)
		if err != nil || !ok {
			t.Errorf("unable to find %v", want.ContentID)
			continue
		}

		if version == 1 {
			// v1 does not preserve original length.
			want = withOriginalLength(want, want.PackedLength-fakeEncryptionOverhead)
		}

		require.Equal(t, want, info2)
	}

	cnt := 0

	require.NoError(t, ndx.Iterate(AllIDs, func(info2 Info) error {
		want := infoMap[info2.ContentID]
		if version == 1 {
			// v1 does not preserve original length.
			want = withOriginalLength(want, want.PackedLength-fakeEncryptionOverhead)
		}

		require.Equal(t, want, info2)
		cnt++
		return nil
	}))

	if cnt != len(infoMap) {
		t.Errorf("invalid number of iterations: %v, wanted %v", cnt, len(infoMap))
	}

	prefixes := []IDPrefix{"a", "b", "f", "0", "3", "aa", "aaa", "aab", "fff", "m", "x", "y", "m0", "ma"}

	for i := range 100 {
		contentID := deterministicContentID(t, "no-such-content", i)

		var v Info

		ok, err := ndx.GetInfo(contentID, &v)
		if err != nil {
			t.Errorf("unable to get content %v: %v", contentID, err)
		}

		if ok {
			t.Errorf("unexpected result when getting content %v: %v", contentID, v)
		}
	}

	for _, prefix := range prefixes {
		cnt2 := 0
		require.NoError(t, ndx.Iterate(PrefixRange(prefix), func(info2 Info) error {
			cnt2++
			if !strings.HasPrefix(info2.ContentID.String(), string(prefix)) {
				t.Errorf("unexpected item %v when iterating prefix %v", info2.ContentID, prefix)
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
			require.NoError(t, buildV2(b.sortedContents(), &result))

			pi, err := Open(result.Bytes(), nil, func() int { return fakeEncryptionOverhead })
			require.NoError(t, err)

			var got Info

			ok, err := pi.GetInfo(cid, &got)
			require.NoError(t, err)
			require.True(t, ok)

			require.Equal(t, got, tc.info)
		} else {
			err := buildV2(b.sortedContents(), &result)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		}
	}
}

func TestSortedContents(t *testing.T) {
	b := Builder{}

	addDeterministicContents(t, b.Add)
	verifySortedEntries(t, b.sortedContents)
}

func TestSortedContentsDifferentPrefixes(t *testing.T) {
	b := Builder{}

	addContentIDsWithDifferentPrefixes(t, b.Add)
	verifySortedEntries(t, b.sortedContents)
}

func TestSortedContentsSingleUse(t *testing.T) {
	b := NewOneUseBuilder()

	addDeterministicContents(t, b.Add)
	verifySortedEntries(t, b.sortedContents)
}

func TestSortedContentsSingleUseDifferentPrefixes(t *testing.T) {
	b := NewOneUseBuilder()

	addContentIDsWithDifferentPrefixes(t, b.Add)
	verifySortedEntries(t, b.sortedContents)
}

func addContentIDsWithDifferentPrefixes(t *testing.T, add func(Info)) {
	t.Helper()

	for _, id := range []string{"0123", "1023", "0f23", "f023", "g0123", "g1023", "i0123", "i1023", "h0123", "h1023"} {
		add(Info{
			ContentID: mustParseID(t, id),
		})
	}
}

func addDeterministicContents(t *testing.T, add func(Info)) {
	t.Helper()

	for i := range 100 {
		add(Info{
			ContentID: deterministicContentID(t, "", i),
		})
	}
}

func addIntsAsDeterministicContent(t *testing.T, ints []int, add func(Info)) {
	t.Helper()

	for i := range ints {
		add(Info{
			ContentID: deterministicContentID(t, "", i),
		})
	}
}

func verifySortedEntries(t *testing.T, sortedContents func() []*Info) {
	t.Helper()

	got := sortedContents()

	var last ID

	for _, info := range got {
		if info.ContentID.less(last) {
			t.Fatalf("not sorted %v (last was %v)!", info.ContentID, last)
		}

		last = info.ContentID
	}
}

func TestPackIndexV2TooManyUniqueFormats(t *testing.T) {
	b := Builder{}

	for i := range v2MaxFormatCount {
		v := deterministicContentID(t, "", i)

		b.Add(Info{
			ContentID:           v,
			PackBlobID:          blob.ID(v.String()),
			FormatVersion:       1,
			CompressionHeaderID: compression.HeaderID(1000 + i),
		})
	}

	require.NoError(t, buildV2(b.sortedContents(), io.Discard))

	// add one more to push it over the edge
	b.Add(Info{
		ContentID:           deterministicContentID(t, "", v2MaxFormatCount),
		FormatVersion:       1,
		CompressionHeaderID: compression.HeaderID(5000),
	})

	err := buildV2(b.sortedContents(), io.Discard)
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
		_ = ndx.Iterate(AllIDs, func(cb Info) error {
			if cnt < 10 {
				var tmp Info

				_, _ = ndx.GetInfo(cb.ContentID, &tmp)
			}
			cnt++
			return nil
		})
	})
}

func fuzzTest(rnd *rand.Rand, originalData []byte, rounds int, callback func(d []byte)) {
	for range rounds {
		data := append([]byte(nil), originalData...)

		// mutate small number of bytes
		bytesToMutate := rnd.Intn(3)
		for range bytesToMutate {
			pos := rnd.Intn(len(data))
			data[pos] = byte(rnd.Int())
		}

		sectionsToInsert := rnd.Intn(3)
		for range sectionsToInsert {
			pos := rnd.Intn(len(data))
			insertedLength := rnd.Intn(20)
			insertedData := make([]byte, insertedLength)
			rnd.Read(insertedData)

			data = append(append(append([]byte(nil), data[0:pos]...), insertedData...), data[pos:]...)
		}

		sectionsToDelete := rnd.Intn(3)
		for range sectionsToDelete {
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

	// generate IDs in random order
	ids := rand.Perm(10_000)

	// add ID to the builder
	addIntsAsDeterministicContent(t, ids, b.Add)

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
	for i := range numTotal {
		m[deterministicContentID(t, "", i)] = true
	}

	cnt := 0

	var lens []int

	for _, s := range sharded {
		cnt += len(s)
		lens = append(lens, len(s))

		for _, v := range s {
			delete(m, v.ContentID)
		}
	}

	require.Equal(t, numTotal, cnt, "invalid total number of sharded elements")
	require.Empty(t, m)

	return lens
}

func TestSingleUseBuilderShard(t *testing.T) {
	// generate IDs in random order
	ids := rand.Perm(10_000)

	cases := []struct {
		shardSize int
		numShards int
		shardLens []int
	}{
		{100000, 1, nil},
		{100, 100, nil},
		{500, 20, []int{460, 472, 473, 477, 479, 483, 486, 492, 498, 499, 501, 503, 504, 505, 511, 519, 524, 528, 542, 544}},
		{1000, 10, []int{945, 964, 988, 988, 993, 1002, 1014, 1017, 1021, 1068}},
		{2000, 5, []int{1952, 1995, 2005, 2013, 2035}},
	}

	for _, tc := range cases {
		b := NewOneUseBuilder()

		addIntsAsDeterministicContent(t, ids, b.Add)

		length := b.Length()
		shards := b.shard(tc.shardSize)

		// verify number of shards
		lens := verifyAllShardedIDsList(t, shards, length, tc.numShards)

		require.Zero(t, b.Length())

		// sharding will always produce stable results, verify sorted shard lengths here
		if tc.shardLens != nil {
			require.ElementsMatch(t, tc.shardLens, lens)
		}
	}
}

func verifyAllShardedIDsList(t *testing.T, sharded [][]*Info, numTotal, numShards int) []int {
	t.Helper()

	require.Len(t, sharded, numShards)

	m := map[ID]bool{}
	for i := range numTotal {
		m[deterministicContentID(t, "", i)] = true
	}

	cnt := 0

	var lens []int

	for _, s := range sharded {
		cnt += len(s)
		lens = append(lens, len(s))

		for _, v := range s {
			delete(m, v.ContentID)
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
