package object

import (
	"bytes"
	"context"
	"crypto/md5"
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"reflect"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/kopia/kopia/block"

	"github.com/kopia/kopia/internal/config"
	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/storage"
)

type fakeBlockManager struct {
	mu   sync.Mutex
	data map[block.ContentID][]byte
}

func (f *fakeBlockManager) GetBlock(ctx context.Context, blockID block.ContentID) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if d, ok := f.data[blockID]; ok {
		return append([]byte(nil), d...), nil
	}

	return nil, storage.ErrBlockNotFound
}

func (f *fakeBlockManager) WriteBlock(ctx context.Context, data []byte, prefix block.ContentID) (block.ContentID, error) {
	h := md5.New()
	h.Write(data)
	blockID := prefix + block.ContentID(hex.EncodeToString(h.Sum(nil)))

	f.mu.Lock()
	defer f.mu.Unlock()

	f.data[blockID] = append([]byte(nil), data...)
	return blockID, nil
}

func (f *fakeBlockManager) BlockInfo(ctx context.Context, blockID block.ContentID) (block.Info, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if d, ok := f.data[blockID]; ok {
		return block.Info{BlockID: blockID, Length: int64(len(d))}, nil
	}

	return block.Info{}, storage.ErrBlockNotFound
}

func (f *fakeBlockManager) Flush(ctx context.Context) error {
	return nil
}

func setupTest(t *testing.T) (map[block.ContentID][]byte, *Manager) {
	return setupTestWithData(t, map[block.ContentID][]byte{}, ManagerOptions{})
}

func setupTestWithData(t *testing.T, data map[block.ContentID][]byte, opts ManagerOptions) (map[block.ContentID][]byte, *Manager) {
	r, err := NewObjectManager(context.Background(), &fakeBlockManager{data: data}, config.RepositoryObjectFormat{
		FormattingOptions: block.FormattingOptions{
			Version: 1,
		},
		MaxBlockSize: 200,
		Splitter:     "FIXED",
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
			ID{ContentBlockID: "77add1d5f41223d5582fca736a5cb335"},
		},
		{make([]byte, 100), ID{ContentBlockID: "6d0bb00954ceb7fbee436bb55a8397a9"}}, // 100 zero bytes
	}

	for _, c := range cases {
		data, om := setupTest(t)

		writer := om.NewWriter(ctx, WriterOptions{})

		writer.Write(c.data)

		result, err := writer.Result()
		if err != nil {
			t.Errorf("error getting writer results for %v, expected: %v", c.data, c.objectID.String())
			continue
		}

		om.writeBackWG.Wait()

		if !objectIDsEqual(result, c.objectID) {
			t.Errorf("incorrect result for %v, expected: %v got: %v", c.data, c.objectID.String(), result.String())
		}

		if c.objectID.ContentBlockID == "" {
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

func objectIDsEqual(o1 ID, o2 ID) bool {
	return reflect.DeepEqual(o1, o2)
}

func TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	ctx := context.Background()
	_, om := setupTest(t)

	bytes := make([]byte, 100)
	writer := om.NewWriter(ctx, WriterOptions{})
	writer.Write(bytes[0:50])
	writer.Write(bytes[0:50])
	result, err := writer.Result()
	if !objectIDsEqual(result, ID{ContentBlockID: "6d0bb00954ceb7fbee436bb55a8397a9"}) {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func verifyIndirectBlock(ctx context.Context, t *testing.T, r *Manager, oid ID) {
	for oid.Indirect != nil {
		direct := *oid.Indirect
		oid = direct

		rd, err := r.Open(ctx, direct)
		if err != nil {
			t.Errorf("unable to open %v: %v", oid.String(), err)
			return
		}
		defer rd.Close()

		pr, err := jsonstream.NewReader(rd, indirectStreamType, nil)
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
	ctx := context.Background()
	cases := []struct {
		dataLength          int
		expectedBlockCount  int
		expectedIndirection int
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
		data, om := setupTest(t)

		contentBytes := make([]byte, c.dataLength)

		writer := om.NewWriter(ctx, WriterOptions{})
		writer.Write(contentBytes)
		result, err := writer.Result()
		if err != nil {
			t.Errorf("error getting writer results: %v", err)
		}

		if indirectionLevel(result) != c.expectedIndirection {
			t.Errorf("incorrect indirection level for size: %v: %v, expected %v", c.dataLength, indirectionLevel(result), c.expectedIndirection)
		}

		if got, want := len(data), c.expectedBlockCount; got != want {
			t.Errorf("unexpected block count for %v: %v, expected %v", c.dataLength, got, want)
		}

		om.Flush(ctx)

		l, b, err := om.VerifyObject(ctx, result)
		if err != nil {
			t.Errorf("error verifying %q: %v", result, err)
		}

		if got, want := int(l), len(contentBytes); got != want {
			t.Errorf("got invalid byte count for %q: %v, wanted %v", result, got, want)
		}

		if got, want := len(b), c.expectedBlockCount; got != want {
			t.Errorf("invalid block count for %v, got %v, wanted %v", result, got, want)
		}

		verifyIndirectBlock(ctx, t, om, result)
	}
}

func indirectionLevel(oid ID) int {
	if oid.Indirect == nil {
		return 0
	}

	return 1 + indirectionLevel(*oid.Indirect)
}

func TestHMAC(t *testing.T) {
	ctx := context.Background()
	content := bytes.Repeat([]byte{0xcd}, 50)

	_, om := setupTest(t)

	w := om.NewWriter(ctx, WriterOptions{})
	w.Write(content)
	result, err := w.Result()
	if result.String() != "D999732b72ceff665b3f7608411db66a4" {
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
		{"Da76999788386641a3ec798554f1fe7e6", storedPayload},
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

	objectID, err := ParseID("Dno-such-block")
	if err != nil {
		t.Errorf("cannot parse object ID: %v", err)
	}
	reader, err := om.Open(ctx, objectID)
	if err != storage.ErrBlockNotFound || reader != nil {
		t.Errorf("unexpected result: reader: %v err: %v", reader, err)
	}
}

func TestEndToEndReadAndSeek(t *testing.T) {
	ctx := context.Background()
	_, om := setupTest(t)

	for _, size := range []int{1, 199, 200, 201, 9999, 512434} {
		// Create some random data sample of the specified size.
		randomData := make([]byte, size)
		cryptorand.Read(randomData)

		writer := om.NewWriter(ctx, WriterOptions{})
		writer.Write(randomData)
		objectID, err := writer.Result()
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
