package repo

import (
	"bytes"
	"crypto/md5"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"reflect"
	"testing"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/internal/storagetesting"
)

func init() {
	panicOnBufferLeaks = true
}

func testFormat() *Format {
	return &Format{
		Version:                1,
		MaxBlockSize:           200,
		MaxInlineContentLength: 20,
		ObjectFormat:           "TESTONLY_MD5",
	}
}

func getMd5Digest(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func getMd5CObjectID(data []byte) string {
	return fmt.Sprintf("D%s", getMd5Digest(data))
}

func getMd5LObjectID(data []byte) string {
	return fmt.Sprintf("L%s", getMd5Digest(data))
}

func setupTest(t *testing.T) (data map[string][]byte, repo *Repository) {
	data = map[string][]byte{}
	st := storagetesting.NewMapStorage(data)

	repo, err := New(st, testFormat(), WriteBack(5))
	if err != nil {
		t.Errorf("cannot create manager: %v", err)
	}
	return
}

func TestWriters(t *testing.T) {
	cases := []struct {
		data     []byte
		objectID ObjectID
	}{
		{[]byte{}, NullObjectID},
		{[]byte("quick brown fox"), ObjectID{TextContent: "quick brown fox"}},
		{[]byte("foo\nbar"), ObjectID{TextContent: "foo\nbar"}},
		{[]byte("\n\n\n"), ObjectID{BinaryContent: []byte{0xa, 0xa, 0xa}}}, // base64 is more efficient than JSON here (4 chars vs 6)
		{[]byte{1, 2, 3, 4}, ObjectID{BinaryContent: []byte{1, 2, 3, 4}}},
		{[]byte{0xc2, 0x28}, ObjectID{BinaryContent: []byte{0xc2, 0x28}}},
		{
			[]byte("the quick brown fox jumps over the lazy dog"),
			ObjectID{StorageBlock: "X77add1d5f41223d5582fca736a5cb335"},
		},
		{make([]byte, 100), ObjectID{StorageBlock: "X6d0bb00954ceb7fbee436bb55a8397a9"}}, // 100 zero bytes
	}

	for _, c := range cases {
		data, repo := setupTest(t)

		writer := repo.NewWriter(
			WithBlockNamePrefix("X"),
		)

		writer.Write(c.data)

		result, err := writer.Result(false)
		if err != nil {
			t.Errorf("error getting writer results for %v, expected: %v", c.data, c.objectID.String())
			continue
		}

		repo.Flush()

		if !objectIDsEqual(result, c.objectID) {
			t.Errorf("incorrect result for %v, expected: %v got: %v %#v", c.data, c.objectID.String(), result.String(), result.BinaryContent)
		}

		if c.objectID.StorageBlock == "" {
			if len(data) != 0 {
				t.Errorf("unexpected data written to the storage: %v", data)
			}
		} else {
			if len(data) != 1 {
				t.Errorf("unexpected data written to the storage: %v", data)
			}
		}
	}
}

func objectIDsEqual(o1 ObjectID, o2 ObjectID) bool {
	return reflect.DeepEqual(o1, o2)
}

func TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	_, repo := setupTest(t)

	bytes := make([]byte, 100)
	writer := repo.NewWriter(
		WithBlockNamePrefix("X"),
	)
	writer.Write(bytes[0:50])
	writer.Write(bytes[0:50])
	result, err := writer.Result(false)
	if !objectIDsEqual(result, ObjectID{StorageBlock: "X6d0bb00954ceb7fbee436bb55a8397a9"}) {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func verifyIndirectBlock(t *testing.T, r *Repository, oid ObjectID) {
	for level := int32(0); level < oid.Indirect; level++ {
		direct := oid
		direct.Indirect = level

		rd, err := r.Open(direct)
		if err != nil {
			t.Errorf("unable to open %v: %v", oid.String(), err)
			return
		}
		defer rd.Close()

		pr, err := jsonstream.NewReader(rd, indirectStreamType)
		if err != nil {
			t.Errorf("cannot open indirect stream: %v", err)
			return
		}
		for {
			v := indirectObjectEntry{}
			if err := pr.Read(&v); err != nil {
				if err == io.EOF {
					break
				}
				t.Errorf("err: %v", err)
				break
			}
		}
	}
}

func TestIndirection(t *testing.T) {
	cases := []struct {
		dataLength          int
		expectedBlockCount  int
		expectedIndirection int32
	}{
		{dataLength: 200, expectedBlockCount: 1, expectedIndirection: 0},
		{dataLength: 250, expectedBlockCount: 3, expectedIndirection: 1},
		{dataLength: 1400, expectedBlockCount: 7, expectedIndirection: 3},
		{dataLength: 2000, expectedBlockCount: 8, expectedIndirection: 3},
		{dataLength: 3000, expectedBlockCount: 9, expectedIndirection: 3},
		{dataLength: 4000, expectedBlockCount: 14, expectedIndirection: 4},
		{dataLength: 10000, expectedBlockCount: 25, expectedIndirection: 4},
	}

	for _, c := range cases {
		data, repo := setupTest(t)

		contentBytes := make([]byte, c.dataLength)

		writer := repo.NewWriter()
		writer.Write(contentBytes)
		result, err := writer.Result(false)
		repo.Flush()
		if err != nil {
			t.Errorf("error getting writer results: %v", err)
		}

		if result.Indirect != c.expectedIndirection {
			t.Errorf("incorrect indirection level for size: %v: %v, expected %v", c.dataLength, result.Indirect, c.expectedIndirection)
		}

		if len(data) != c.expectedBlockCount {
			t.Errorf("unexpected block count for %v: %v, expected %v", c.dataLength, len(data), c.expectedBlockCount)
		}

		b, err := repo.GetStorageBlocks(result)
		if err != nil {
			t.Errorf("error getting storage blocks for %q: %v", result, err)
		}

		if got, want := len(b), c.expectedBlockCount; got != want {
			t.Errorf("invalid block count for %v, got %v, wanted %v", result, got, want)
		}

		verifyIndirectBlock(t, repo, result)
	}
}

func TestHMAC(t *testing.T) {
	data := map[string][]byte{}
	content := bytes.Repeat([]byte{0xcd}, 50)

	s := testFormat()
	s.ObjectFormat = "TESTONLY_MD5"

	repo, err := New(storagetesting.NewMapStorage(data), s)
	if err != nil {
		t.Errorf("cannot create manager: %v", err)
	}
	w := repo.NewWriter()
	w.Write(content)
	result, err := w.Result(false)
	if result.String() != "D999732b72ceff665b3f7608411db66a4" {
		t.Errorf("unexpected result: %v err: %v", result.String(), err)
	}
}

func TestReader(t *testing.T) {
	data, repo := setupTest(t)

	storedPayload := []byte("foo\nbar")
	data["a76999788386641a3ec798554f1fe7e6"] = storedPayload

	cases := []struct {
		text    string
		payload []byte
	}{
		{"B", []byte{}},
		{"BAQIDBA", []byte{1, 2, 3, 4}},
		{"TFoo", []byte("Foo")},
		{"Da76999788386641a3ec798554f1fe7e6", storedPayload},
		{"S0,2,BAQIDBA", []byte{1, 2}},
		{"S1,3,BAQIDBA", []byte{2, 3, 4}},
		{"S1,5,BAQIDBA", []byte{2, 3, 4}},
		{"S0,0,BAQIDBA", []byte{}},
		{"S0,2,Da76999788386641a3ec798554f1fe7e6", storedPayload[0:2]},
		{"S2,4,Da76999788386641a3ec798554f1fe7e6", storedPayload[2:6]},
	}

	for _, c := range cases {
		objectID, err := ParseObjectID(c.text)
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		reader, err := repo.Open(objectID)
		if err != nil {
			t.Errorf("cannot create reader for %v: %v", objectID, err)
			continue
		}

		d, err := ioutil.ReadAll(reader)
		if err != nil {
			t.Errorf("cannot read all data for %v: %v", objectID, err)
			continue
		}
		if !bytes.Equal(d, c.payload) {
			t.Errorf("incorrect payload for %v: expected: %v got: %v", objectID, c.payload, d)
			continue
		}

	}
}

func TestMalformedStoredData(t *testing.T) {
	data, repo := setupTest(t)

	cases := [][]byte{
		[]byte("foo\nba"),
		[]byte("foo\nbar1"),
	}

	for _, c := range cases {
		data["a76999788386641a3ec798554f1fe7e6"] = c
		objectID, err := ParseObjectID("Da76999788386641a3ec798554f1fe7e6")
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		reader, err := repo.Open(objectID)
		if err == nil || reader != nil {
			t.Errorf("expected error for %x", c)
		}
	}
}

func TestReaderStoredBlockNotFound(t *testing.T) {
	_, repo := setupTest(t)

	objectID, err := ParseObjectID("Dno-such-block")
	if err != nil {
		t.Errorf("cannot parse object ID: %v", err)
	}
	reader, err := repo.Open(objectID)
	if err != blob.ErrBlockNotFound || reader != nil {
		t.Errorf("unexpected result: reader: %v err: %v", reader, err)
	}
}

func TestEndToEndReadAndSeek(t *testing.T) {
	_, repo := setupTest(t)

	for _, forceStored := range []bool{false, true} {
		for _, size := range []int{1, 199, 200, 201, 9999, 512434} {
			// Create some random data sample of the specified size.
			randomData := make([]byte, size)
			cryptorand.Read(randomData)

			writer := repo.NewWriter(
				WithBlockNamePrefix("X"),
			)
			writer.Write(randomData)
			objectID, err := writer.Result(forceStored)
			writer.Close()
			if err != nil {
				t.Errorf("cannot get writer result for %v/%v: %v", forceStored, size, err)
				continue
			}

			verify(t, repo, objectID, randomData, fmt.Sprintf("%v %v/%v", objectID, forceStored, size))

			if size > 1 {
				sectionID := SectionObjectID(0, int64(size/2), objectID)
				verify(t, repo, sectionID, randomData[0:10], fmt.Sprintf("%+v %v/%v", sectionID, forceStored, size))
			}

			if size > 1 {
				sectionID := SectionObjectID(int64(1), int64(size-1), objectID)
				verify(t, repo, sectionID, randomData[1:], fmt.Sprintf("%+v %v/%v", sectionID, forceStored, size))
			}
		}
	}
}

func verify(t *testing.T, repo *Repository, objectID ObjectID, expectedData []byte, testCaseID string) {
	reader, err := repo.Open(objectID)
	if err != nil {
		t.Errorf("cannot get reader for %v: %v", testCaseID, err)
		return
	}

	for i := 0; i < 20; i++ {
		sampleSize := int(rand.Int31n(300))
		seekOffset := int(rand.Int31n(int32(len(expectedData))))
		if seekOffset+sampleSize > len(expectedData) {
			sampleSize = len(expectedData) - seekOffset
		}
		if sampleSize > 0 {
			got := make([]byte, sampleSize)
			if offset, err := reader.Seek(int64(seekOffset), 0); err != nil || offset != int64(seekOffset) {
				t.Errorf("seek error: %v offset=%v expected:%v", err, offset, seekOffset)
			}
			if n, err := reader.Read(got); err != nil || n != sampleSize {
				t.Errorf("invalid data: n=%v, expected=%v, err:%v", n, sampleSize, err)
			}

			expected := expectedData[seekOffset : seekOffset+sampleSize]

			if !bytes.Equal(expected, got) {
				t.Errorf("incorrect data read for %v: expected: %x, got: %x", testCaseID, expected, got)
			}
		}
	}
}

func TestFormats(t *testing.T) {
	makeFormat := func(objectFormat string) *Format {
		return &Format{
			Version:      1,
			ObjectFormat: objectFormat,
			Secret:       []byte("key"),
			MaxBlockSize: 10000,
		}
	}

	cases := []struct {
		format *Format
		oids   map[string]ObjectID
	}{
		{
			format: &Format{
				Version:      1,
				ObjectFormat: "TESTONLY_MD5",
				MaxBlockSize: 10000,
			},
			oids: map[string]ObjectID{
				"": ObjectID{StorageBlock: "d41d8cd98f00b204e9800998ecf8427e"},
				"The quick brown fox jumps over the lazy dog": ObjectID{
					StorageBlock: "9e107d9d372bb6826bd81d3542a419d6",
				},
			},
		},
		{
			format: makeFormat("UNENCRYPTED_HMAC_SHA256"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": ObjectID{
					StorageBlock: "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
				},
			},
		},
		{
			format: makeFormat("UNENCRYPTED_HMAC_SHA256_128"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": ObjectID{
					StorageBlock: "f7bc83f430538424b13298e6aa6fb143",
				},
			},
		},
	}

	for caseIndex, c := range cases {
		data := map[string][]byte{}
		st := storagetesting.NewMapStorage(data)

		repo, err := New(st, c.format)
		if err != nil {
			t.Errorf("cannot create manager: %v", err)
			continue
		}

		for k, v := range c.oids {
			bytesToWrite := []byte(k)
			w := repo.NewWriter()
			w.Write(bytesToWrite)
			oid, err := w.Result(true)
			if err != nil {
				t.Errorf("error: %v", err)
			}
			if !objectIDsEqual(oid, v) {
				t.Errorf("invalid oid for #%v %v/%v:\ngot:\n%#v\nexpected:\n%#v", caseIndex, c.format.ObjectFormat, k, oid.String(), v.String())
			}

			rc, err := repo.Open(oid)
			if err != nil {
				t.Errorf("open failed: %v", err)
				continue
			}
			bytesRead, err := ioutil.ReadAll(rc)
			if err != nil {
				t.Errorf("error reading: %v", err)
			}
			if !bytes.Equal(bytesRead, bytesToWrite) {
				t.Errorf("data mismatch, read:%x vs written:%v", bytesRead, bytesToWrite)
			}
		}
	}
}
