package cas

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

	"github.com/kopia/kopia/blob"
)

func testFormat() Format {
	return Format{
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
	st := blob.NewMapStorage(data)

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
	s.ObjectFormat = "hmac-md5"
	s.Secret = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

	repo, err := NewRepository(blob.NewMapStorage(data), s)
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
			if err != nil {
				t.Errorf("cannot get writer result for %v/%v: %v", forceStored, size, err)
				continue
			}

			reader, err := repo.Open(objectID)
			if err != nil {
				t.Errorf("cannot get reader for %v/%v: %v %v", forceStored, size, objectID, err)
				continue
			}

			for i := 0; i < 20; i++ {
				sampleSize := int(rand.Int31n(300))
				seekOffset := int(rand.Int31n(int32(len(randomData))))
				if seekOffset+sampleSize > len(randomData) {
					sampleSize = len(randomData) - seekOffset
				}
				if sampleSize > 0 {
					got := make([]byte, sampleSize)
					reader.Seek(int64(seekOffset), 0)
					reader.Read(got)

					expected := randomData[seekOffset : seekOffset+sampleSize]

					if !bytes.Equal(expected, got) {
						t.Errorf("incorrect data read for %v/%v: expected: %v, got: %v", forceStored, size, expected, got)
					}
				}
			}
		}
	}
}

func TestFormats(t *testing.T) {
	cases := []struct {
		format Format
		oids   map[string]ObjectID
	}{
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "md5",
			},
			oids: map[string]ObjectID{
				"": "Dd41d8cd98f00b204e9800998ecf8427e",
				"The quick brown fox jumps over the lazy dog": "D9e107d9d372bb6826bd81d3542a419d6",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-md5",
			},
			oids: map[string]ObjectID{
				"": "D74e6f7298a9c2d168935f58c001bad88",
				"The quick brown fox jumps over the lazy dog": "Dad262969c53bc16032f160081c4a07a0",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-md5",
				Secret:       []byte("key"),
			},
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "D80070713463e7749b90c2dc24911e275",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-sha1",
				Secret:       []byte("key"),
			},
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Dde7c9b85b8b78aa6bc8a7a36f70a90701c9db4d9",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-sha256",
				Secret:       []byte("key"),
			},
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Df7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-sha512",
				Secret:       []byte("key"),
			},
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Db42af09057bac1e2d41708e48a902e09b5ff7f12ab428a4fe86653c73dd248fb82f948a549f7b791a5b41915ee4d1ec3935357e4e2317250d0372afa2ebeeb3a",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-sha256-aes128",
				Secret:       []byte("key"),
			},
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Df7bc83f430538424b13298e6aa6fb143:ef4d59a14946175997479dbc2d1a3cd8",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-sha384-aes256",
				Secret:       []byte("key"),
			},
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Dd7f4727e2c0b39ae0f1e40cc96f60242:d5b7801841cea6fc592c5d3e1ae50700582a96cf35e1e554995fe4e03381c237",
			},
		},
		{
			format: Format{
				Version:      "1",
				ObjectFormat: "hmac-sha512-aes256",
				Secret:       []byte("key"),
			},
			oids: map[string]ObjectID{
				"The quick brown fox jumps over the lazy dog": "Db42af09057bac1e2d41708e48a902e09b5ff7f12ab428a4fe86653c73dd248fb:82f948a549f7b791a5b41915ee4d1ec3935357e4e2317250d0372afa2ebeeb3a",
			},
		},
	}

	for _, c := range cases {
		data := map[string][]byte{}
		st := blob.NewMapStorage(data)

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
				t.Errorf("invalid oid for %v/%v: %v expected %v", c.format.ObjectFormat, k, oid, v)
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
	st := blob.NewMapStorage(data)
	format := Format{
		Version:      "1",
		ObjectFormat: "hmac-sha512-aes256",
		Secret:       []byte("key"),
	}

	repo, err := NewRepository(st, format)
	if err != nil {
		t.Errorf("cannot create manager: %v", err)
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
