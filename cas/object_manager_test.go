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

	"github.com/kopia/kopia/content"
	"github.com/kopia/kopia/storage"
)

func testFormat() Format {
	return Format{
		MaxBlobSize:       200,
		MaxInlineBlobSize: 20,
		Algorithm:         "md5",
	}
}

func getMd5Digest(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func getMd5CObjectID(data []byte) string {
	return fmt.Sprintf("C%s", getMd5Digest(data))
}

func getMd5LObjectID(data []byte) string {
	return fmt.Sprintf("L%s", getMd5Digest(data))
}

func setupTest(t *testing.T) (data map[string][]byte, mgr ObjectManager) {
	data = map[string][]byte{}
	st := storage.NewMapRepository(data)

	mgr, err := NewObjectManager(st, testFormat())
	if err != nil {
		t.Errorf("cannot create manager: %v", err)
	}
	return
}

func TestWriters(t *testing.T) {
	cases := []struct {
		data     []byte
		objectID content.ObjectID
	}{
		{[]byte{}, "B"},
		{[]byte("quick brown fox"), "Tquick brown fox"},
		{[]byte{1, 2, 3, 4}, "BAQIDBA=="},
		{[]byte{0xc2, 0x28}, "Bwig="}, // invalid UTF-8, will be represented as binary
		{
			[]byte("the quick brown fox jumps over the lazy dog"),
			"CX77add1d5f41223d5582fca736a5cb335",
		},
		{make([]byte, 100), "CX6d0bb00954ceb7fbee436bb55a8397a9"}, // 100 zero bytes
	}

	for _, c := range cases {
		data, mgr := setupTest(t)

		writer := mgr.NewWriter(
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
				t.Errorf("unexpected data written to the repository: %v", data)
			}
		} else {
			if len(data) != 1 {
				t.Errorf("unexpected data written to the repository: %v", data)
			}
		}
	}

}

func TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	_, mgr := setupTest(t)

	bytes := make([]byte, 100)
	writer := mgr.NewWriter(
		WithBlockNamePrefix("X"),
	)
	writer.Write(bytes[0:50])
	writer.Write(bytes[0:50])
	result, err := writer.Result(false)
	if string(result) != "CX6d0bb00954ceb7fbee436bb55a8397a9" {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func TestWriterListChunk(t *testing.T) {
	data, mgr := setupTest(t)

	contentBytes := make([]byte, 250)
	contentMd5Sum200 := getMd5Digest(contentBytes[0:200])  // hash of 200 zero bytes
	contentMd5Sum50 := getMd5Digest(contentBytes[200:250]) // hash of 50 zero bytes
	listChunkContent := []byte("200,C" + contentMd5Sum200 + "\n50,C" + contentMd5Sum50 + "\n")
	listChunkObjectID := getMd5LObjectID(listChunkContent)

	writer := mgr.NewWriter()
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
		t.Errorf("invalid repository contents: %v", data)
	}
}

func TestWriterListOfListsChunk(t *testing.T) {
	data, mgr := setupTest(t)

	contentBytes := make([]byte, 1400)
	chunk1Id := getMd5CObjectID(contentBytes[0:200]) // hash of 200 zero bytes

	list1ChunkContent := []byte(strings.Repeat("200,"+chunk1Id+"\n", 5))
	list1ObjectID := fmt.Sprintf("%v,%v", len(list1ChunkContent), getMd5LObjectID(list1ChunkContent))

	list2ChunkContent := []byte(strings.Repeat("200,"+chunk1Id+"\n", 2))
	list2ObjectID := fmt.Sprintf("%v,%v", len(list2ChunkContent), getMd5LObjectID(list2ChunkContent))

	listOfListsChunkContent := []byte(list1ObjectID + "\n" + list2ObjectID + "\n")
	listOfListsObjectID := getMd5LObjectID(listOfListsChunkContent)

	writer := mgr.NewWriter()
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
		t.Errorf("invalid repository contents: %v", data)
	}
}

func TestWriterListOfListsOfListsChunk(t *testing.T) {
	data, mgr := setupTest(t)

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

	writer := mgr.NewWriter()
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
		t.Errorf("invalid repository contents: %v", data)
	}
}

func TestHMAC(t *testing.T) {
	data := map[string][]byte{}
	content := bytes.Repeat([]byte{0xcd}, 50)

	s := testFormat()
	s.Algorithm = "hmac-md5"
	s.Secret = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}

	mgr, err := NewObjectManager(storage.NewMapRepository(data), s)
	if err != nil {
		t.Errorf("cannot create manager: %v", err)
	}
	w := mgr.NewWriter()
	w.Write(content)
	result, err := w.Result(false)
	if string(result) != "C697eaf0aca3a3aea3a75164746ffaa79" {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func TestReader(t *testing.T) {
	data, mgr := setupTest(t)

	storedPayload := []byte("foo\nbar")
	data["abcdef"] = storedPayload

	cases := []struct {
		text    string
		payload []byte
	}{
		{"B", []byte{}},
		{"BAQIDBA==", []byte{1, 2, 3, 4}},
		{"T", []byte{}},
		{"Tfoo\nbar", []byte("foo\nbar")},
		{"Cabcdef", storedPayload},
	}

	for _, c := range cases {
		objectID, err := content.ParseObjectID(c.text)
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		reader, err := mgr.Open(objectID)
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

func TestReaderStoredBlockNotFound(t *testing.T) {
	_, mgr := setupTest(t)

	objectID, err := content.ParseObjectID("Cno-such-block")
	if err != nil {
		t.Errorf("cannot parse object ID: %v", err)
	}
	reader, err := mgr.Open(objectID)
	if err != storage.ErrBlockNotFound || reader != nil {
		t.Errorf("unexpected result: reader: %v err: %v", reader, err)
	}
}

func TestEndToEndReadAndSeek(t *testing.T) {
	_, mgr := setupTest(t)

	for _, forceStored := range []bool{false, true} {
		for _, size := range []int{1, 199, 200, 201, 9999, 512434} {
			// Create some random data sample of the specified size.
			randomData := make([]byte, size)
			cryptorand.Read(randomData)

			writer := mgr.NewWriter(
				WithBlockNamePrefix("X"),
			)
			writer.Write(randomData)
			objectID, err := writer.Result(forceStored)
			if err != nil {
				t.Errorf("cannot get writer result for %v/%v: %v", forceStored, size, err)
				continue
			}

			reader, err := mgr.Open(objectID)
			if err != nil {
				t.Errorf("cannot get reader for %v/%v: %v", forceStored, size, err)
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
		hashes map[string]content.ObjectID
	}{
		{
			format: Format{
				Algorithm: "md5",
			},
			hashes: map[string]content.ObjectID{
				"": "Cd41d8cd98f00b204e9800998ecf8427e",
				"The quick brown fox jumps over the lazy dog": "C9e107d9d372bb6826bd81d3542a419d6",
			},
		},
		{
			format: Format{
				Algorithm: "hmac-md5",
			},
			hashes: map[string]content.ObjectID{
				"": "C74e6f7298a9c2d168935f58c001bad88",
				"The quick brown fox jumps over the lazy dog": "Cad262969c53bc16032f160081c4a07a0",
			},
		},
		{
			format: Format{
				Algorithm: "hmac-md5",
				Secret:    []byte("key"),
			},
			hashes: map[string]content.ObjectID{
				"The quick brown fox jumps over the lazy dog": "C80070713463e7749b90c2dc24911e275",
			},
		},
		{
			format: Format{
				Algorithm: "hmac-sha1",
				Secret:    []byte("key"),
			},
			hashes: map[string]content.ObjectID{
				"The quick brown fox jumps over the lazy dog": "Cde7c9b85b8b78aa6bc8a7a36f70a90701c9db4d9",
			},
		},
		{
			format: Format{
				Algorithm: "hmac-sha256",
				Secret:    []byte("key"),
			},
			hashes: map[string]content.ObjectID{
				"The quick brown fox jumps over the lazy dog": "Cf7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8",
			},
		},
		{
			format: Format{
				Algorithm: "hmac-sha512",
				Secret:    []byte("key"),
			},
			hashes: map[string]content.ObjectID{
				"The quick brown fox jumps over the lazy dog": "Cb42af09057bac1e2d41708e48a902e09b5ff7f12ab428a4fe86653c73dd248fb82f948a549f7b791a5b41915ee4d1ec3935357e4e2317250d0372afa2ebeeb3a",
			},
		},
	}

	for _, c := range cases {
		data := map[string][]byte{}
		st := storage.NewMapRepository(data)

		mgr, err := NewObjectManager(st, c.format)
		if err != nil {
			t.Errorf("cannot create manager: %v", err)
			continue
		}

		for k, v := range c.hashes {
			w := mgr.NewWriter()
			w.Write([]byte(k))
			oid, err := w.Result(true)
			if err != nil {
				t.Errorf("error: %v", err)
			}
			if oid != v {
				t.Errorf("invalid oid for %v/%v: %v expected %v", c.format.Algorithm, k, oid, v)
			}
		}
	}
}
