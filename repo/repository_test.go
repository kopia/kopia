package repo

import (
	"bytes"
	"crypto/md5"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/storagetesting"
)

func testFormat() *Format {
	return &Format{
		Version:           "1",
		MaxBlobSize:       200,
		MaxInlineBlobSize: 20,
		ObjectFormat:      "md5",
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

func setupTest(t *testing.T) (data map[string][]byte, repo Repository) {
	data = map[string][]byte{}
	st := storagetesting.NewMapStorage(data)

	repo, err := NewRepository(st, testFormat())
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
		{[]byte{}, "B"},
		{[]byte("quick brown fox"), "Tquick brown fox"},
		{[]byte{1, 2, 3, 4}, "BAQIDBA"},
		{[]byte{10, 13, 9}, "T\n\r\t"},
		{[]byte{0xc2, 0x28}, "Bwig"}, // invalid UTF-8, will be represented as binary
		{
			[]byte("the quick brown fox jumps over the lazy dog"),
			"DX77add1d5f41223d5582fca736a5cb335",
		},
		{make([]byte, 100), "DX6d0bb00954ceb7fbee436bb55a8397a9"}, // 100 zero bytes
	}

	for _, c := range cases {
		data, repo := setupTest(t)

		writer := repo.NewWriter(
			WithBlockNamePrefix("X"),
		)

		writer.Write(c.data)

		result, err := writer.Result(false)
		if err != nil {
			t.Errorf("error getting writer results for %v, expected: %v", c.data, c.objectID)
			continue
		}

		if result != c.objectID {
			t.Errorf("incorrect result for %v, expected: %v got: %v", c.data, c.objectID, result)
		}

		if !c.objectID.Type().IsStored() {
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

func TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	_, repo := setupTest(t)

	bytes := make([]byte, 100)
	writer := repo.NewWriter(
		WithBlockNamePrefix("X"),
	)
	writer.Write(bytes[0:50])
	writer.Write(bytes[0:50])
	result, err := writer.Result(false)
	if string(result) != "DX6d0bb00954ceb7fbee436bb55a8397a9" {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func TestWriterListChunk(t *testing.T) {
	data, repo := setupTest(t)

	contentBytes := make([]byte, 250)
	contentMd5Sum200 := getMd5Digest(contentBytes[0:200])  // hash of 200 zero bytes
	contentMd5Sum50 := getMd5Digest(contentBytes[200:250]) // hash of 50 zero bytes
	listChunkContent := []byte("200,D" + contentMd5Sum200 + "\n50,D" + contentMd5Sum50 + "\n")
	listChunkObjectID := getMd5LObjectID(listChunkContent)

	writer := repo.NewWriter()
	writer.Write(contentBytes)
	result, err := writer.Result(false)
	if err != nil {
		t.Errorf("error getting writer results: %v", err)
	}

	if string(result) != listChunkObjectID {
		t.Errorf("incorrect list chunk ID: %v, expected: %v", result, listChunkObjectID)
	}

	// We have 3 chunks - 200 zero bytes, 50 zero bytes and the list.
	if !reflect.DeepEqual(data, map[string][]byte{
		contentMd5Sum200:               contentBytes[0:200],
		contentMd5Sum50:                contentBytes[0:50],
		getMd5Digest(listChunkContent): listChunkContent,
	}) {
		t.Errorf("invalid storage contents: %v", data)
	}
}

func TestWriterListOfListsChunk(t *testing.T) {
	data, repo := setupTest(t)

	contentBytes := make([]byte, 1400)
	chunk1Id := getMd5CObjectID(contentBytes[0:200]) // hash of 200 zero bytes

	list1ChunkContent := []byte(strings.Repeat("200,"+chunk1Id+"\n", 5))
	list1ObjectID := fmt.Sprintf("%v,%v", len(list1ChunkContent), getMd5LObjectID(list1ChunkContent))

	list2ChunkContent := []byte(strings.Repeat("200,"+chunk1Id+"\n", 2))
	list2ObjectID := fmt.Sprintf("%v,%v", len(list2ChunkContent), getMd5LObjectID(list2ChunkContent))

	listOfListsChunkContent := []byte(list1ObjectID + "\n" + list2ObjectID + "\n")
	listOfListsObjectID := getMd5LObjectID(listOfListsChunkContent)

	writer := repo.NewWriter()
	writer.Write(contentBytes)
	result, err := writer.Result(false)
	if string(result) != listOfListsObjectID || err != nil {
		t.Errorf("unexpected result: %v expected: %v, err: %v", result, listOfListsObjectID, err)
	}

	// We have 4 chunks - 200 zero bytes, 2 lists, and 1 list-of-lists.
	if !reflect.DeepEqual(data, map[string][]byte{
		getMd5Digest(contentBytes[0:200]):     contentBytes[0:200],
		getMd5Digest(list1ChunkContent):       list1ChunkContent,
		getMd5Digest(list2ChunkContent):       list2ChunkContent,
		getMd5Digest(listOfListsChunkContent): listOfListsChunkContent,
	}) {
		t.Errorf("invalid storage contents: %v", data)
	}
}

func TestWriterListOfListsOfListsChunk(t *testing.T) {
	data, repo := setupTest(t)

	writtenData := make([]byte, 10000)
	chunk1Id := getMd5CObjectID(writtenData[0:200]) // hash of 200 zero bytes

	// First level list chunk has 5 C[] chunk IDs, because that's how many IDs fit in one chunk.
	// (that number 200 was chosen for a reason, to make testing easy)
	//
	//   200,Cfbaf48ec981a5eecdb57b929fdd426e8\n
	//   200,Cfbaf48ec981a5eecdb57b929fdd426e8\n
	//   200,Cfbaf48ec981a5eecdb57b929fdd426e8\n
	//   200,Cfbaf48ec981a5eecdb57b929fdd426e8\n
	//   200,Cfbaf48ec981a5eecdb57b929fdd426e8\n
	list1ChunkContent := []byte(strings.Repeat("200,"+chunk1Id+"\n", 5))
	list1ObjectID := fmt.Sprintf("%v,%v", len(list1ChunkContent), getMd5LObjectID(list1ChunkContent))

	// Second level lists L[] chunks from the first level. They have all the same content
	// because all lists are identical.
	//   190,L52760f658059fef754f5deabdd01df93\n
	//   190,L52760f658059fef754f5deabdd01df93\n
	//   190,L52760f658059fef754f5deabdd01df93\n
	//   190,L52760f658059fef754f5deabdd01df93\n
	//   190,L52760f658059fef754f5deabdd01df93\n
	list2ChunkContent := []byte(strings.Repeat(string(list1ObjectID)+"\n", 5))
	list2ObjectID := fmt.Sprintf("%v,%v", len(list2ChunkContent), getMd5LObjectID(list2ChunkContent))

	// Now those lists are also identical and represent 5000 bytes each, so
	// the top-level list-of-lists-of-lists will have 2 entries:
	//
	//   190,Lb99b28e34c87e4934b4cc5631bb38ee8\n
	//   190,Lb99b28e34c87e4934b4cc5631bb38ee8\n
	list3ChunkContent := []byte(strings.Repeat(string(list2ObjectID)+"\n", 2))
	list3ObjectID := getMd5LObjectID(list3ChunkContent)

	writer := repo.NewWriter()
	writer.Write(writtenData)

	result, err := writer.Result(false)
	if string(result) != list3ObjectID {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}

	// We have 4 data blocks representing 10000 bytes of zero. Not bad!
	if !reflect.DeepEqual(data, map[string][]byte{
		getMd5Digest(writtenData[0:200]): writtenData[0:200],
		getMd5Digest(list1ChunkContent):  list1ChunkContent,
		getMd5Digest(list2ChunkContent):  list2ChunkContent,
		getMd5Digest(list3ChunkContent):  list3ChunkContent,
	}) {
		t.Errorf("invalid storage contents: %v", data)
	}
}

func TestHMAC(t *testing.T) {
	data := map[string][]byte{}
	content := bytes.Repeat([]byte{0xcd}, 50)

	s := testFormat()
	s.ObjectFormat = "md5"
	s.Secret = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

	repo, err := NewRepository(storagetesting.NewMapStorage(data), s)
	if err != nil {
		t.Errorf("cannot create manager: %v", err)
	}
	w := repo.NewWriter()
	w.Write(content)
	result, err := w.Result(false)
	if string(result) != "D697eaf0aca3a3aea3a75164746ffaa79" {
		t.Errorf("unexpected result: %v err: %v", result, err)
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
		{"T", []byte{}},
		{"Tfoo\nbar", []byte("foo\nbar")},
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
	if err != storage.ErrBlockNotFound || reader != nil {
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
			if err != nil {
				t.Errorf("cannot get writer result for %v/%v: %v", forceStored, size, err)
				continue
			}

			verify(t, repo, objectID, randomData, fmt.Sprintf("%v %v/%v", objectID, forceStored, size))

			if size > 1 {
				sectionID := NewSectionObjectID(0, int64(size/2), objectID)
				verify(t, repo, sectionID, randomData[0:10], fmt.Sprintf("%v %v/%v", sectionID, forceStored, size))
			}

			if size > 1 {
				sectionID := NewSectionObjectID(int64(1), int64(size-1), objectID)
				verify(t, repo, sectionID, randomData[1:], fmt.Sprintf("%v %v/%v", sectionID, forceStored, size))
			}
		}
	}
}

func verify(t *testing.T, repo Repository, objectID ObjectID, expectedData []byte, testCaseID string) {
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
			Version:      "1",
			ObjectFormat: objectFormat,
			Secret:       []byte("key"),
			MaxBlobSize:  10000,
		}
	}

	cases := []struct {
		format *Format
		oids   map[string]ObjectID
	}{
		{
			format: makeFormat("md5"), // MD5-HMAC with secret 'key'
			oids: map[string]ObjectID{
				"": "D63530468a04e386459855da0063b6596",
				"The quick brown fox jumps over the lazy dog": "D80070713463e7749b90c2dc24911e275",
			},
		},
		{
			format: &Format{
				Version:      "1",
				ObjectFormat: "md5",
				MaxBlobSize:  10000,
				Secret:       []byte{}, // HMAC with zero-byte secret
			},
			oids: map[string]ObjectID{
				"": "D74e6f7298a9c2d168935f58c001bad88",
				"The quick brown fox jumps over the lazy dog": "Dad262969c53bc16032f160081c4a07a0",
			},
		},
		{
			format: &Format{
				Version:      "1",
				ObjectFormat: "md5",
				MaxBlobSize:  10000,
				Secret:       nil, // non-HMAC version
			},
			oids: map[string]ObjectID{
				"": "Dd41d8cd98f00b204e9800998ecf8427e",
				"The quick brown fox jumps over the lazy dog": "D9e107d9d372bb6826bd81d3542a419d6",
			},
		},
		{
			format: makeFormat("md5"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "D80070713463e7749b90c2dc24911e275",
			},
		},
		{
			format: makeFormat("sha1"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Dde7c9b85b8b78aa6bc8a7a36f70a90701c9db4d9",
			},
		},
		{
			format: makeFormat("sha256"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Df7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
			},
		},
		{
			format: makeFormat("sha512"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Db42af09057bac1e2d41708e48a902e09b5ff7f12ab428a4fe86653c73dd248fb82f948a549f7b791a5b41915ee4d1ec3935357e4e2317250d0372afa2ebeeb3a",
			},
		},
		{
			format: makeFormat("sha256-fold128-aes128"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "D028e74c630aee6400db4f84d49d1ed08.2825273c3344999e26081d99bcaf3f18",
			},
		},
		{
			format: makeFormat("sha256-aes128"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "D26bb3fee9e83a7f6ff8397f33460cd0d24354b28ae2d41b6f2376fbe7db12005.2825273c3344999e26081d99bcaf3f18",
			},
		},
		{
			format: makeFormat("sha384-aes256"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "D08edca658caee5ae86c9f1e4ccea26b1a43b0ddb04502c5f1b1cba051a756a9eba440bb6991b23323e37556ec5cf4f88.7a753377319650870f5cba47c30b608675542f206fa67d94dcbb9a0ccce7f467",
			},
		},
		{
			format: makeFormat("sha512-aes256"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Deee04ad71248ef4ffec5159558e80e2791e32299cfb79a1e063cc5f4a71cd61ca2c1b110e3ae2e83635a8626a3a27e4805eb745c40b5a4ebd4c9372602e5ab65.1c0b1b58ce05b7b8b05cfce27a485ddf97bde5159f6946357ec7795236f36a84",
			},
		},
		{
			format: makeFormat("sha512-fold128-aes192"),
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Dd829ad027ee4ff394f6a61615eb30d16.0ab00e0e0e156927f0be63372e8449d7bf2316c43d46e626",
			},
		},
	}

	for caseIndex, c := range cases {
		data := map[string][]byte{}
		st := storagetesting.NewMapStorage(data)

		t.Logf("verifying %#v", c.format)
		repo, err := NewRepository(st, c.format)
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
			if oid != v {
				t.Errorf("invalid oid for #%v %v/%v: %v expected %v", caseIndex, c.format.ObjectFormat, k, oid, v)
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

func TestInvalidEncryptionKey(t *testing.T) {
	data := map[string][]byte{}
	st := storagetesting.NewMapStorage(data)
	format := Format{
		Version:      "1",
		ObjectFormat: "sha512-aes256",
		Secret:       []byte("key"),
		MaxBlobSize:  1000,
	}

	repo, err := NewRepository(st, &format)
	if err != nil {
		t.Errorf("cannot create manager: %v", err)
		return
	}

	bytesToWrite := make([]byte, 1024)
	for i := range bytesToWrite {
		bytesToWrite[i] = byte(i)
	}

	w := repo.NewWriter()
	w.Write(bytesToWrite)
	oid, err := w.Result(true)
	if err != nil {
		t.Errorf("error: %v", err)
	}

	rc, err := repo.Open(oid)
	if err != nil || rc == nil {
		t.Errorf("error opening valid ObjectID: %v", err)
		return
	}

	// Key too short
	rc, err = repo.Open(oid[0 : len(oid)-2])
	if err == nil || rc != nil {
		t.Errorf("expected error when opening malformed object")
	}

	// Key too long
	rc, err = repo.Open(oid + "ff")
	if err == nil || rc != nil {
		t.Errorf("expected error when opening malformed object")
	}

	// Invalid key
	lastByte, _ := hex.DecodeString(string(oid[len(oid)-2:]))
	lastByte[0]++
	rc, err = repo.Open(oid[0:len(oid)-2] + ObjectID(hex.EncodeToString(lastByte)))
	if err == nil || rc != nil {
		t.Errorf("expected error when opening malformed object: %v", err)
	}

	// Now corrupt the data
	data[string(oid.BlockID())][0] ^= 1
	rc, err = repo.Open(oid)
	if err == nil || rc != nil {
		t.Errorf("expected error when opening object with corrupt data")
	}
}
