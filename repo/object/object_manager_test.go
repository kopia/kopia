package object

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type fakeContentManager struct {
	mu   sync.Mutex
	data map[content.ID][]byte
}

func (f *fakeContentManager) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if d, ok := f.data[contentID]; ok {
		return append([]byte(nil), d...), nil
	}

	return nil, content.ErrContentNotFound
}

func (f *fakeContentManager) WriteContent(ctx context.Context, data []byte, prefix content.ID) (content.ID, error) {
	h := sha256.New()
	h.Write(data) //nolint:errcheck
	contentID := prefix + content.ID(hex.EncodeToString(h.Sum(nil)))

	f.mu.Lock()
	defer f.mu.Unlock()

	f.data[contentID] = append([]byte(nil), data...)

	return contentID, nil
}

func (f *fakeContentManager) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if d, ok := f.data[contentID]; ok {
		return content.Info{ID: contentID, Length: uint32(len(d))}, nil
	}

	return content.Info{}, blob.ErrBlobNotFound
}

func (f *fakeContentManager) Flush(ctx context.Context) error {
	return nil
}

func setupTest(t *testing.T) (map[content.ID][]byte, *Manager) {
	return setupTestWithData(t, map[content.ID][]byte{}, ManagerOptions{})
}

func setupTestWithData(t *testing.T, data map[content.ID][]byte, opts ManagerOptions) (map[content.ID][]byte, *Manager) {
	r, err := NewObjectManager(context.Background(), &fakeContentManager{data: data}, Format{
		Splitter: "FIXED-1M",
	}, opts)
	if err != nil {
		t.Fatalf("can't create object manager: %v", err)
	}

	return data, r
}

func TestWriters(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		data     []byte
		objectID ID
	}{
		{
			[]byte("the quick brown fox jumps over the lazy dog"),
			"05c6e08f1d9fdafa03147fcb8f82f124c76d2f70e3d989dc8aadb5e7d7450bec",
		},
		{make([]byte, 100), "cd00e292c5970d3c5e2f0ffa5171e555bc46bfc4faddfb4a418b6840b86e79a3"}, // 100 zero bytes
	}

	for _, c := range cases {
		data, om := setupTest(t)

		writer := om.NewWriter(ctx, WriterOptions{})

		if _, err := writer.Write(c.data); err != nil {
			t.Errorf("write error: %v", err)
		}

		result, err := writer.Result()
		if err != nil {
			t.Errorf("error getting writer results for %v, expected: %v", c.data, c.objectID.String())
			continue
		}

		if !objectIDsEqual(result, c.objectID) {
			t.Errorf("incorrect result for %v, expected: %v got: %v", c.data, c.objectID.String(), result.String())
		}

		if _, ok := c.objectID.ContentID(); !ok {
			if len(data) != 0 {
				t.Errorf("unexpected data written to the storage: %v", data)
			}
		} else {
			if len(data) != 1 {
				// 1 data block
				t.Errorf("unexpected data written to the storage: %v", data)
			}
		}
	}
}

func objectIDsEqual(o1, o2 ID) bool {
	return o1 == o2
}

func TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	ctx := context.Background()
	_, om := setupTest(t)

	b := make([]byte, 100)
	writer := om.NewWriter(ctx, WriterOptions{})
	writer.Write(b[0:50]) //nolint:errcheck
	writer.Write(b[0:50]) //nolint:errcheck
	result, err := writer.Result()

	if !objectIDsEqual(result, "cd00e292c5970d3c5e2f0ffa5171e555bc46bfc4faddfb4a418b6840b86e79a3") {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func verifyIndirectBlock(ctx context.Context, t *testing.T, r *Manager, oid ID) {
	for indexBlobID, isIndirect := oid.IndexObjectID(); isIndirect; indexBlobID, isIndirect = indexBlobID.IndexObjectID() {
		rd, err := r.Open(ctx, indexBlobID)
		if err != nil {
			t.Errorf("unable to open %v: %v", oid.String(), err)
			return
		}
		defer rd.Close()

		var ind indirectObject
		if err := json.NewDecoder(rd).Decode(&ind); err != nil {
			t.Errorf("cannot parse indirect stream: %v", err)
		}
	}
}

func TestIndirection(t *testing.T) {
	ctx := context.Background()

	splitterFactory := newFixedSplitterFactory(1000)
	cases := []struct {
		dataLength          int
		expectedBlobCount   int
		expectedIndirection int
	}{
		{dataLength: 200, expectedBlobCount: 1, expectedIndirection: 0},
		{dataLength: 1000, expectedBlobCount: 1, expectedIndirection: 0},
		{dataLength: 1001, expectedBlobCount: 3, expectedIndirection: 1},
		// 1 blob of 1000 zeros, 1 blob of 5 zeros + 1 index blob
		{dataLength: 3005, expectedBlobCount: 3, expectedIndirection: 1},
		// 1 blob of 1000 zeros + 1 index blob
		{dataLength: 4000, expectedBlobCount: 2, expectedIndirection: 1},
		// 1 blob of 1000 zeros + 1 index blob
		{dataLength: 10000, expectedBlobCount: 2, expectedIndirection: 1},
	}

	for _, c := range cases {
		data, om := setupTest(t)

		contentBytes := make([]byte, c.dataLength)

		writer := om.NewWriter(ctx, WriterOptions{})
		writer.(*objectWriter).splitter = splitterFactory()

		if _, err := writer.Write(contentBytes); err != nil {
			t.Errorf("write error: %v", err)
		}

		result, err := writer.Result()
		if err != nil {
			t.Errorf("error getting writer results: %v", err)
		}

		t.Logf("len %v got %v", len(contentBytes), result)

		if indirectionLevel(result) != c.expectedIndirection {
			t.Errorf("incorrect indirection level for size: %v: %v, expected %v", c.dataLength, indirectionLevel(result), c.expectedIndirection)
		}

		if got, want := len(data), c.expectedBlobCount; got != want {
			t.Errorf("unexpected blob count for %v: %v, expected %v", c.dataLength, got, want)
		}

		b, err := om.VerifyObject(ctx, result)
		if err != nil {
			t.Errorf("error verifying %q: %v", result, err)
		}

		if got, want := len(b), c.expectedBlobCount; got != want {
			t.Errorf("invalid blob count for %v, got %v, wanted %v", result, got, want)
		}

		verifyIndirectBlock(ctx, t, om, result)
	}
}

func indirectionLevel(oid ID) int {
	indexObjectID, ok := oid.IndexObjectID()
	if !ok {
		return 0
	}

	return 1 + indirectionLevel(indexObjectID)
}

func TestHMAC(t *testing.T) {
	ctx := context.Background()
	c := bytes.Repeat([]byte{0xcd}, 50)

	_, om := setupTest(t)

	w := om.NewWriter(ctx, WriterOptions{})
	w.Write(c) //nolint:errcheck
	result, err := w.Result()

	if result.String() != "cad29ff89951a3c085c86cb7ed22b82b51f7bdfda24f932c7f9601f51d5975ba" {
		t.Errorf("unexpected result: %v err: %v", result.String(), err)
	}
}

func TestReader(t *testing.T) {
	ctx := context.Background()
	data, om := setupTest(t)

	storedPayload := []byte("foo\nbar")
	data["a76999788386641a3ec798554f1fe7e6"] = storedPayload

	cases := []struct {
		text    string
		payload []byte
	}{
		{"a76999788386641a3ec798554f1fe7e6", storedPayload},
	}

	for _, c := range cases {
		objectID, err := ParseID(c.text)
		if err != nil {
			t.Errorf("cannot parse object ID: %v", err)
			continue
		}

		reader, err := om.Open(ctx, objectID)
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
	ctx := context.Background()
	_, om := setupTest(t)

	objectID, err := ParseID("deadbeef")
	if err != nil {
		t.Errorf("cannot parse object ID: %v", err)
	}

	reader, err := om.Open(ctx, objectID)
	if err != ErrObjectNotFound || reader != nil {
		t.Errorf("unexpected result: reader: %v err: %v", reader, err)
	}
}

func TestEndToEndReadAndSeek(t *testing.T) {
	ctx := context.Background()
	_, om := setupTest(t)

	for _, size := range []int{1, 199, 200, 201, 9999, 512434} {
		// Create some random data sample of the specified size.
		randomData := make([]byte, size)
		cryptorand.Read(randomData) //nolint:errcheck

		writer := om.NewWriter(ctx, WriterOptions{})
		if _, err := writer.Write(randomData); err != nil {
			t.Errorf("write error: %v", err)
		}

		objectID, err := writer.Result()
		t.Logf("oid: %v", objectID)

		writer.Close()

		if err != nil {
			t.Errorf("cannot get writer result for %v: %v", size, err)
			continue
		}

		verify(ctx, t, om, objectID, randomData, fmt.Sprintf("%v %v", objectID, size))
	}
}

func verify(ctx context.Context, t *testing.T, om *Manager, objectID ID, expectedData []byte, testCaseID string) {
	t.Helper()

	reader, err := om.Open(ctx, objectID)
	if err != nil {
		t.Errorf("cannot get reader for %v (%v): %v %v", testCaseID, objectID, err, string(debug.Stack()))
		return
	}

	// nolint:dupl
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

// nolint:gocyclo
func TestSeek(t *testing.T) {
	ctx := context.Background()
	_, om := setupTest(t)

	for _, size := range []int{0, 1, 500000, 15000000} {
		randomData := make([]byte, size)
		cryptorand.Read(randomData) //nolint:errcheck

		writer := om.NewWriter(ctx, WriterOptions{})
		if _, err := writer.Write(randomData); err != nil {
			t.Errorf("write error: %v", err)
		}

		objectID, err := writer.Result()
		if err != nil {
			t.Fatalf("unable to write: %v", err)
		}

		r, err := om.Open(ctx, objectID)
		if err != nil {
			t.Fatalf("open error: %v", err)
		}

		if pos, err := r.Seek(0, io.SeekStart); err != nil || pos != 0 {
			t.Errorf("invalid seek-start result %v %v", pos, err)
		}

		if pos, err := r.Seek(0, io.SeekCurrent); err != nil || pos != 0 {
			t.Errorf("invalid seek-current at start result %v %v", pos, err)
		}

		if pos, err := r.Seek(0, io.SeekEnd); err != nil || pos != int64(size) {
			t.Errorf("invalid seek-end result %v %v", pos, err)
		}

		if pos, err := r.Seek(0, io.SeekCurrent); err != nil || pos != int64(size) {
			t.Errorf("invalid seek-current at end result %v %v, wanted %v", pos, err, size)
		}

		if pos, err := r.Seek(1, io.SeekCurrent); err != nil || pos != int64(size)+1 {
			t.Errorf("unexpected result when seeking past end of file: %v, %v, wanted %v", pos, err, size+1)
		}

		buf := make([]byte, 5)
		if n, err := r.Read(buf); n != 0 || err != io.EOF {
			t.Errorf("unexpected read result %v %v", n, err)
		}
	}
}
