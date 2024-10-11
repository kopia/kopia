package object

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/impossible"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/splitter"
)

var errSomeError = errors.New("some error")

type fakeContentManager struct {
	mu sync.Mutex

	// +checklocks:mu
	data map[content.ID][]byte
	// +checklocks:mu
	compresionIDs map[content.ID]compression.HeaderID

	supportsContentCompression bool
	writeContentError          error
}

func (f *fakeContentManager) PrefetchContents(ctx context.Context, contentIDs []content.ID, hint string) []content.ID {
	return contentIDs
}

func (f *fakeContentManager) GetContent(ctx context.Context, contentID content.ID) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if d, ok := f.data[contentID]; ok {
		return append([]byte(nil), d...), nil
	}

	return nil, content.ErrContentNotFound
}

func (f *fakeContentManager) WriteContent(ctx context.Context, data gather.Bytes, prefix content.IDPrefix, comp compression.HeaderID) (content.ID, error) {
	if f.writeContentError != nil {
		return content.EmptyID, f.writeContentError
	}

	h := sha256.New()
	data.WriteTo(h)
	contentID, err := content.IDFromHash(prefix, h.Sum(nil))
	impossible.PanicOnError(err)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.data[contentID] = data.ToByteSlice()
	if f.compresionIDs != nil {
		f.compresionIDs[contentID] = comp
	}

	return contentID, nil
}

func (f *fakeContentManager) SupportsContentCompression() bool {
	return f.supportsContentCompression
}

func (f *fakeContentManager) ContentInfo(ctx context.Context, contentID content.ID) (content.Info, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if d, ok := f.data[contentID]; ok {
		return content.Info{ContentID: contentID, PackedLength: uint32(len(d)), CompressionHeaderID: f.compresionIDs[contentID]}, nil
	}

	return content.Info{}, blob.ErrBlobNotFound
}

func (f *fakeContentManager) Flush(ctx context.Context) error {
	return nil
}

func setupTest(t *testing.T, compressionHeaderID map[content.ID]compression.HeaderID) (map[content.ID][]byte, *fakeContentManager, *Manager) {
	t.Helper()

	data := map[content.ID][]byte{}

	fcm := &fakeContentManager{
		data:                       data,
		supportsContentCompression: compressionHeaderID != nil,
		compresionIDs:              compressionHeaderID,
	}

	r, err := NewObjectManager(testlogging.Context(t), fcm, format.ObjectFormat{
		Splitter: "FIXED-1M",
	}, nil)
	if err != nil {
		t.Fatalf("can't create object manager: %v", err)
	}

	return data, fcm, r
}

func TestWriters(t *testing.T) {
	ctx := testlogging.Context(t)
	cases := []struct {
		data     []byte
		objectID ID
	}{
		{
			[]byte("the quick brown fox jumps over the lazy dog"),
			mustParseID(t, "05c6e08f1d9fdafa03147fcb8f82f124c76d2f70e3d989dc8aadb5e7d7450bec"),
		},
		{make([]byte, 100), mustParseID(t, "cd00e292c5970d3c5e2f0ffa5171e555bc46bfc4faddfb4a418b6840b86e79a3")}, // 100 zero bytes
	}

	for _, c := range cases {
		data, _, om := setupTest(t, nil)

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

		if _, _, ok := c.objectID.ContentID(); !ok {
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

func TestCompression_ContentCompressionEnabled(t *testing.T) {
	ctx := testlogging.Context(t)

	cmap := map[content.ID]compression.HeaderID{}
	_, _, om := setupTest(t, cmap)

	w := om.NewWriter(ctx, WriterOptions{
		Compressor:         "gzip",
		MetadataCompressor: "zstd-fastest",
	})
	w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1000))
	oid, err := w.Result()
	require.NoError(t, err)

	cid, isCompressed, ok := oid.ContentID()

	require.True(t, ok)
	require.False(t, isCompressed) // oid will not indicate compression
	require.Equal(t, compression.ByName["gzip"].HeaderID(), cmap[cid])
}

func TestCompression_IndirectContentCompressionEnabledMetadata(t *testing.T) {
	ctx := testlogging.Context(t)

	cmap := map[content.ID]compression.HeaderID{}
	_, _, om := setupTest(t, cmap)
	w := om.NewWriter(ctx, WriterOptions{
		Compressor:         "gzip",
		MetadataCompressor: "zstd-fastest",
	})
	w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1000000))
	oid, err := w.Result()
	require.NoError(t, err)
	verifyIndirectBlock(ctx, t, om, oid, compression.HeaderZstdFastest)

	w2 := om.NewWriter(ctx, WriterOptions{
		MetadataCompressor: "none",
	})
	w2.Write(bytes.Repeat([]byte{5, 6, 7, 8}, 1000000))
	oid2, err2 := w2.Result()
	require.NoError(t, err2)
	verifyIndirectBlock(ctx, t, om, oid2, content.NoCompression)
}

func TestCompression_CustomSplitters(t *testing.T) {
	cases := []struct {
		wo          WriterOptions
		wantLengths []int64
	}{
		{
			wo:          WriterOptions{Splitter: ""},
			wantLengths: []int64{1048576, 393216}, // uses default FIXED-1M
		},
		{
			wo:          WriterOptions{Splitter: "nosuchsplitter"},
			wantLengths: []int64{1048576, 393216}, // falls back to default FIXED-1M
		},
		{
			wo:          WriterOptions{Splitter: "FIXED-128K"},
			wantLengths: []int64{131072, 131072, 131072, 131072, 131072, 131072, 131072, 131072, 131072, 131072, 131072},
		},
		{
			wo:          WriterOptions{Splitter: "FIXED-256K"},
			wantLengths: []int64{262144, 262144, 262144, 262144, 262144, 131072},
		},
	}

	ctx := testlogging.Context(t)

	for _, tc := range cases {
		cmap := map[content.ID]compression.HeaderID{}
		_, fcm, om := setupTest(t, cmap)

		w := om.NewWriter(ctx, tc.wo)

		w.Write(bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 128<<10))
		oid, err := w.Result()
		require.NoError(t, err)

		ndx, ok := oid.IndexObjectID()
		require.True(t, ok)

		entries, err := LoadIndexObject(ctx, fcm, ndx)
		require.NoError(t, err)

		var gotLengths []int64
		for _, e := range entries {
			gotLengths = append(gotLengths, e.Length)
		}

		require.Equal(t, tc.wantLengths, gotLengths)
	}
}

func TestCompression_ContentCompressionDisabled(t *testing.T) {
	ctx := testlogging.Context(t)

	// this disables content compression
	_, _, om := setupTest(t, nil)

	w := om.NewWriter(ctx, WriterOptions{
		Compressor:         "gzip",
		MetadataCompressor: "zstd-fastest",
	})
	w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1000))
	oid, err := w.Result()
	require.NoError(t, err)

	_, isCompressed, ok := oid.ContentID()
	require.True(t, ok)
	require.True(t, isCompressed) // oid will indicate compression
}

func TestWriterCompleteChunkInTwoWrites(t *testing.T) {
	ctx := testlogging.Context(t)
	_, _, om := setupTest(t, nil)

	b := make([]byte, 100)
	writer := om.NewWriter(ctx, WriterOptions{})
	writer.Write(b[0:50])
	writer.Write(b[0:50])
	result, err := writer.Result()

	if !objectIDsEqual(result, mustParseID(t, "cd00e292c5970d3c5e2f0ffa5171e555bc46bfc4faddfb4a418b6840b86e79a3")) {
		t.Errorf("unexpected result: %v err: %v", result, err)
	}
}

func TestCheckpointing(t *testing.T) {
	ctx := testlogging.Context(t)
	_, _, om := setupTest(t, nil)

	writer := om.NewWriter(ctx, WriterOptions{})

	// write all zeroes
	allZeroes := make([]byte, 1<<20)

	// empty file, nothing flushed
	checkpoint1, err := writer.Checkpoint()
	verifyNoError(t, err)

	// write some bytes, but not enough to flush.
	writer.Write(allZeroes[0:50])
	checkpoint2, err := writer.Checkpoint()
	verifyNoError(t, err)

	// write enough to flush first content.
	writer.Write(allZeroes)
	checkpoint3, err := writer.Checkpoint()
	verifyNoError(t, err)

	// write enough to flush second content.
	writer.Write(allZeroes)
	checkpoint4, err := writer.Checkpoint()
	verifyNoError(t, err)

	result, err := writer.Result()
	verifyNoError(t, err)

	if !objectIDsEqual(checkpoint1, EmptyID) {
		t.Errorf("unexpected checkpoint1: %v err: %v", checkpoint1, err)
	}

	if !objectIDsEqual(checkpoint2, EmptyID) {
		t.Errorf("unexpected checkpoint2: %v err: %v", checkpoint2, err)
	}

	result2, err := writer.Checkpoint()
	verifyNoError(t, err)

	if result2 != result {
		t.Errorf("invalid checkpoint after result: %v vs %v", result2, result)
	}

	verifyFull(ctx, t, om, checkpoint3, allZeroes)
	verifyFull(ctx, t, om, checkpoint4, make([]byte, 2<<20))
	verifyFull(ctx, t, om, result, make([]byte, 2<<20+50))
}

func TestObjectWriterRaceBetweenCheckpointAndResult(t *testing.T) {
	ctx := testlogging.Context(t)
	data := map[content.ID][]byte{}
	fcm := &fakeContentManager{
		data: data,
	}

	om, err := NewObjectManager(testlogging.Context(t), fcm, format.ObjectFormat{
		Splitter: "FIXED-1M",
	}, nil)
	if err != nil {
		t.Fatalf("can't create object manager: %v", err)
	}

	allZeroes := make([]byte, 1<<20-5)

	repeat := 100
	if testutil.ShouldReduceTestComplexity() {
		repeat = 5
	}

	for range repeat {
		w := om.NewWriter(ctx, WriterOptions{
			AsyncWrites: 1,
		})

		w.Write(allZeroes)
		w.Write(allZeroes)
		w.Write(allZeroes)

		var eg errgroup.Group

		eg.Go(func() error {
			_, rerr := w.Result()

			return rerr
		})

		eg.Go(func() error {
			cpID, cperr := w.Checkpoint()
			if cperr == nil && cpID != EmptyID {
				ids, verr := VerifyObject(ctx, om.contentMgr, cpID)
				if verr != nil {
					return errors.Wrapf(err, "Checkpoint() returned invalid object %v", cpID)
				}

				for _, id := range ids {
					if id == content.EmptyID {
						return errors.New("checkpoint returned empty id")
					}
				}
			}

			return nil
		})

		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

func verifyFull(ctx context.Context, t *testing.T, om *Manager, oid ID, want []byte) {
	t.Helper()

	r, err := Open(ctx, om.contentMgr, oid)
	if err != nil {
		t.Fatalf("unable to open %v: %v", oid, err)
	}

	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unable to read all: %v", err)
	}

	if !bytes.Equal(data, want) {
		t.Fatalf("unexpected data read for %v", oid)
	}
}

func verifyNoError(t *testing.T, err error) {
	t.Helper()

	require.NoError(t, err)
}

func verifyIndirectBlock(ctx context.Context, t *testing.T, om *Manager, oid ID, expectedComp compression.HeaderID) {
	t.Helper()

	for indexContentID, isIndirect := oid.IndexObjectID(); isIndirect; indexContentID, isIndirect = indexContentID.IndexObjectID() {
		func() {
			if c, _, ok := indexContentID.ContentID(); ok {
				if !c.HasPrefix() {
					t.Errorf("expected base content ID to be prefixed, was %v", c)
				}
				info, err := om.contentMgr.ContentInfo(ctx, c)
				if err != nil {
					t.Errorf("error getting content info for %v", err.Error())
				}
				require.Equal(t, expectedComp, info.CompressionHeaderID)
			}

			rd, err := Open(ctx, om.contentMgr, indexContentID)
			if err != nil {
				t.Errorf("unable to open %v: %v", oid.String(), err)
				return
			}
			defer rd.Close()

			var ind indirectObject
			if err := json.NewDecoder(rd).Decode(&ind); err != nil {
				t.Errorf("cannot parse indirect stream: %v", err)
			}
		}()
	}
}

func TestIndirection(t *testing.T) {
	ctx := testlogging.Context(t)

	splitterFactory := splitter.Fixed(1000)
	cases := []struct {
		dataLength          int
		expectedBlobCount   int
		expectedIndirection int
		metadataCompressor  compression.Name
	}{
		{dataLength: 200, expectedBlobCount: 1, expectedIndirection: 0},
		{dataLength: 1000, expectedBlobCount: 1, expectedIndirection: 0},
		{dataLength: 1001, expectedBlobCount: 3, expectedIndirection: 1},
		// 1 blob of 1000 zeros, 1 blob of 5 zeros + 1 index blob
		{dataLength: 3005, expectedBlobCount: 3, expectedIndirection: 1},
		// 1 blob of 1000 zeros + 1 index blob
		{dataLength: 4000, expectedBlobCount: 2, expectedIndirection: 1},
		// 1 blob of 1000 zeros + 1 index blob
		{dataLength: 10000, expectedBlobCount: 2, expectedIndirection: 1, metadataCompressor: "none"},
		// 1 blob of 1000 zeros + 1 index blob, enabled metadata compression
		{dataLength: 10000, expectedBlobCount: 2, expectedIndirection: 1, metadataCompressor: "zstd-fastest"},
	}

	for _, c := range cases {
		cmap := map[content.ID]compression.HeaderID{}
		data, _, om := setupTest(t, cmap)

		contentBytes := make([]byte, c.dataLength)

		writer := om.NewWriter(ctx, WriterOptions{MetadataCompressor: c.metadataCompressor})
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

		b, err := VerifyObject(ctx, om.contentMgr, result)
		if err != nil {
			t.Errorf("error verifying %q: %v", result, err)
		}

		if got, want := len(b), c.expectedBlobCount; got != want {
			t.Errorf("invalid blob count for %v, got %v, wanted %v", result, got, want)
		}

		expectedCompressor := content.NoCompression
		if len(c.metadataCompressor) > 0 && c.metadataCompressor != "none" {
			expectedCompressor = compression.ByName[c.metadataCompressor].HeaderID()
		}
		verifyIndirectBlock(ctx, t, om, result, expectedCompressor)
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
	ctx := testlogging.Context(t)
	c := bytes.Repeat([]byte{0xcd}, 50)

	_, _, om := setupTest(t, nil)

	w := om.NewWriter(ctx, WriterOptions{})
	w.Write(c)
	result, err := w.Result()

	if result.String() != "cad29ff89951a3c085c86cb7ed22b82b51f7bdfda24f932c7f9601f51d5975ba" {
		t.Errorf("unexpected result: %v err: %v", result.String(), err)
	}
}

//nolint:gocyclo
func TestConcatenate(t *testing.T) {
	ctx := testlogging.Context(t)
	_, _, om := setupTest(t, nil)

	phrase := []byte("hello world\n")
	phraseLength := len(phrase)
	shortRepeatCount := 17
	longRepeatCount := 999999

	emptyObject := mustWriteObject(t, om, nil, "")

	// short uncompressed object - <content>
	shortUncompressedOID := mustWriteObject(t, om, bytes.Repeat(phrase, shortRepeatCount), "")

	// long uncompressed object - Ix<content>
	longUncompressedOID := mustWriteObject(t, om, bytes.Repeat(phrase, longRepeatCount), "")

	// short compressed object - Z<content>
	shortCompressedOID := mustWriteObject(t, om, bytes.Repeat(phrase, shortRepeatCount), "pgzip")

	// long compressed object - Ix<content>
	longCompressedOID := mustWriteObject(t, om, bytes.Repeat(phrase, longRepeatCount), "pgzip")

	if _, compressed, ok := shortUncompressedOID.ContentID(); !ok || compressed {
		t.Errorf("invalid test assumption - shortUncompressedOID %v", shortUncompressedOID)
	}

	if _, isIndex := longUncompressedOID.IndexObjectID(); !isIndex {
		t.Errorf("invalid test assumption - longUncompressedOID %v", longUncompressedOID)
	}

	if _, compressed, ok := shortCompressedOID.ContentID(); !ok || !compressed {
		t.Errorf("invalid test assumption - shortCompressedOID %v", shortCompressedOID)
	}

	if _, isIndex := longCompressedOID.IndexObjectID(); !isIndex {
		t.Errorf("invalid test assumption - longCompressedOID %v", longCompressedOID)
	}

	shortLength := phraseLength * shortRepeatCount
	longLength := phraseLength * longRepeatCount

	cases := []struct {
		inputs     []ID
		wantLength int
	}{
		{[]ID{emptyObject}, 0},
		{[]ID{shortUncompressedOID}, shortLength},
		{[]ID{longUncompressedOID}, longLength},
		{[]ID{shortCompressedOID}, shortLength},
		{[]ID{longCompressedOID}, longLength},

		{[]ID{shortUncompressedOID, shortUncompressedOID}, 2 * shortLength},
		{[]ID{shortUncompressedOID, shortCompressedOID}, 2 * shortLength},
		{[]ID{emptyObject, longCompressedOID, shortCompressedOID, emptyObject, longCompressedOID, shortUncompressedOID, shortUncompressedOID, emptyObject, emptyObject}, 2*longLength + 3*shortLength},
	}

	for _, tc := range cases {
		concatenatedOID, err := om.Concatenate(ctx, tc.inputs, "zstd-fastest")
		if err != nil {
			t.Fatal(err)
		}

		r, err := Open(ctx, om.contentMgr, concatenatedOID)
		if err != nil {
			t.Fatal(err)
		}

		gotLength := int(r.Length())
		r.Close()

		if gotLength != tc.wantLength {
			t.Errorf("invalid length for %v: %v, want %v", tc.inputs, gotLength, tc.wantLength)
		}

		b := make([]byte, len(phrase))

		// read the concatenated object in buffers the size of a single phrase, each buffer should be identical.
		for n, readerr := r.Read(b); ; n, readerr = r.Read(b) {
			if errors.Is(readerr, io.EOF) {
				break
			}

			if n != len(b) {
				t.Errorf("invalid length: %v", n)
			}

			if !bytes.Equal(b, phrase) {
				t.Errorf("invalid buffer: %v", n)
			}
		}

		if _, err = VerifyObject(ctx, om.contentMgr, concatenatedOID); err != nil {
			t.Fatalf("verify error: %v", err)
		}

		// make sure results of concatenation can be further concatenated.
		concatenated3OID, err := om.Concatenate(ctx, []ID{concatenatedOID, concatenatedOID, concatenatedOID}, "zstd-fastest")
		if err != nil {
			t.Fatal(err)
		}

		r, err = Open(ctx, om.contentMgr, concatenated3OID)
		if err != nil {
			t.Fatal(err)
		}

		gotLength = int(r.Length())
		r.Close()

		if gotLength != tc.wantLength*3 {
			t.Errorf("invalid twice-concatenated object length: %v, want %v", gotLength, tc.wantLength*3)
		}
	}
}

func mustWriteObject(t *testing.T, om *Manager, data []byte, compressor compression.Name) ID {
	t.Helper()

	w := om.NewWriter(testlogging.Context(t), WriterOptions{Compressor: compressor})
	defer w.Close()

	_, err := w.Write(data)
	require.NoError(t, err)

	oid, err := w.Result()
	require.NoError(t, err)

	return oid
}

func TestReader(t *testing.T) {
	ctx := testlogging.Context(t)
	data, _, om := setupTest(t, nil)

	storedPayload := []byte("foo\nbar")

	cid, err := content.ParseID("a76999788386641a3ec798554f1fe7e6")
	require.NoError(t, err)

	data[cid] = storedPayload

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

		reader, err := Open(ctx, om.contentMgr, objectID)
		if err != nil {
			t.Errorf("cannot create reader for %v: %v", objectID, err)
			continue
		}

		d, err := io.ReadAll(reader)
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
	ctx := testlogging.Context(t)
	_, _, om := setupTest(t, nil)

	objectID, err := ParseID("deadbeef")
	if err != nil {
		t.Errorf("cannot parse object ID: %v", err)
	}

	reader, err := Open(ctx, om.contentMgr, objectID)
	if !errors.Is(err, ErrObjectNotFound) || reader != nil {
		t.Errorf("unexpected result: reader: %v err: %v", reader, err)
	}
}

func TestEndToEndReadAndSeek(t *testing.T) {
	for _, asyncWrites := range []int{0, 4, 8} {
		t.Run(fmt.Sprintf("async-%v", asyncWrites), func(t *testing.T) {
			t.Parallel()

			ctx := testlogging.Context(t)
			_, _, om := setupTest(t, nil)

			for _, size := range []int{1, 199, 200, 201, 9999, 512434, 5012434} {
				// Create some random data sample of the specified size.
				randomData := make([]byte, size)
				cryptorand.Read(randomData)

				writer := om.NewWriter(ctx, WriterOptions{AsyncWrites: asyncWrites})
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

				verify(ctx, t, om.contentMgr, objectID, randomData, fmt.Sprintf("%v %v", objectID, size))
			}
		})
	}
}

func TestEndToEndReadAndSeekWithCompression(t *testing.T) {
	sizes := []int{1, 199, 9999, 512434, 5012434, 15000000}

	if runtime.GOARCH != "amd64" {
		sizes = []int{1, 199, 9999, 512434}
	}

	compressibleData := map[int][]byte{}
	nonCompressibleData := map[int][]byte{}

	for _, s := range sizes {
		compressibleData[s] = makeMaybeCompressibleData(s, true)
		nonCompressibleData[s] = makeMaybeCompressibleData(s, false)
	}

	for _, compressible := range []bool{false, true} {
		for compressorName := range compression.ByName {
			t.Run(string(compressorName), func(t *testing.T) {
				ctx := testlogging.Context(t)

				totalBytesWritten := 0

				data, _, om := setupTest(t, nil)

				for _, size := range sizes {
					var inputData []byte

					if compressible {
						inputData = compressibleData[size]
					} else {
						inputData = nonCompressibleData[size]
					}

					writer := om.NewWriter(ctx, WriterOptions{Compressor: compressorName})
					if _, err := writer.Write(inputData); err != nil {
						t.Errorf("write error: %v", err)
					}

					totalBytesWritten += size

					objectID, err := writer.Result()

					writer.Close()

					if err != nil {
						t.Errorf("cannot get writer result for %v: %v", size, err)
						continue
					}

					verify(ctx, t, om.contentMgr, objectID, inputData, fmt.Sprintf("%v %v", objectID, size))
				}

				if compressible {
					compressedBytes := 0
					for _, d := range data {
						compressedBytes += len(d)
					}

					// data is highly compressible, should easily compress to 1% of original size or less
					ratio := float64(compressedBytes) / float64(totalBytesWritten)
					if ratio > 0.01 {
						t.Errorf("compression not effective for %v wrote %v, compressed %v, ratio %v", compressorName, totalBytesWritten, compressedBytes, ratio)
					}
				}
			})
		}
	}
}

func makeMaybeCompressibleData(size int, compressible bool) []byte {
	if compressible {
		phrase := []byte("quick brown fox")
		return append(append([]byte(nil), phrase[0:size%len(phrase)]...), bytes.Repeat(phrase, size/len(phrase))...)
	}

	b := make([]byte, size)
	cryptorand.Read(b)

	return b
}

func verify(ctx context.Context, t *testing.T, cr contentReader, objectID ID, expectedData []byte, testCaseID string) {
	t.Helper()

	reader, err := Open(ctx, cr, objectID)
	if err != nil {
		t.Errorf("cannot get reader for %v (%v): %v %v", testCaseID, objectID, err, string(debug.Stack()))
		return
	}

	for range 20 {
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

//nolint:gocyclo
func TestSeek(t *testing.T) {
	ctx := testlogging.Context(t)
	_, _, om := setupTest(t, nil)

	for _, size := range []int{0, 1, 500000, 15000000} {
		randomData := make([]byte, size)
		cryptorand.Read(randomData)

		writer := om.NewWriter(ctx, WriterOptions{})
		if _, err := writer.Write(randomData); err != nil {
			t.Errorf("write error: %v", err)
		}

		objectID, err := writer.Result()
		if err != nil {
			t.Fatalf("unable to write: %v", err)
		}

		r, err := Open(ctx, om.contentMgr, objectID)
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
		if n, err := r.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
			t.Errorf("unexpected read result %v %v", n, err)
		}
	}
}

func TestWriterFlushFailure_OnWrite(t *testing.T) {
	_, fcm, om := setupTest(t, nil)

	ctx := testlogging.Context(t)
	w := om.NewWriter(ctx, WriterOptions{})

	fcm.writeContentError = errSomeError

	n, err := w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1e6))
	require.ErrorIs(t, err, errSomeError)
	require.Equal(t, 0, n)
}

func TestWriterFlushFailure_OnFlush(t *testing.T) {
	_, fcm, om := setupTest(t, nil)

	ctx := testlogging.Context(t)
	w := om.NewWriter(ctx, WriterOptions{})

	n, err := w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1e6))
	require.NoError(t, err)
	require.Equal(t, 4000000, n)

	fcm.writeContentError = errSomeError

	_, err = w.Result()
	require.ErrorIs(t, err, errSomeError)
}

func TestWriterFlushFailure_OnCheckpoint(t *testing.T) {
	_, fcm, om := setupTest(t, nil)

	ctx := testlogging.Context(t)
	w := om.NewWriter(ctx, WriterOptions{})

	w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1e6))

	fcm.writeContentError = errSomeError
	_, err := w.Checkpoint()

	require.ErrorIs(t, err, errSomeError)
}

func TestWriterFlushFailure_OnAsyncWrite(t *testing.T) {
	_, fcm, om := setupTest(t, nil)

	ctx := testlogging.Context(t)
	w := om.NewWriter(ctx, WriterOptions{
		AsyncWrites: 1,
	})

	fcm.writeContentError = errSomeError

	n, err := w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1e6))
	require.NotErrorIs(t, err, errSomeError)
	require.Equal(t, 4000000, n)

	_, err = w.Result()
	require.ErrorIs(t, err, errSomeError)
}

type faultyCompressor struct{}

func (faultyCompressor) Compress(output io.Writer, input io.Reader) error {
	return errSomeError
}

func (faultyCompressor) Decompress(output io.Writer, input io.Reader, withHeader bool) error {
	return nil
}

func (faultyCompressor) HeaderID() compression.HeaderID {
	return 0
}

func TestWriterFailure_OnCompression(t *testing.T) {
	_, _, om := setupTest(t, nil)

	compression.RegisterCompressor("faulty", &faultyCompressor{})

	ctx := testlogging.Context(t)
	w := om.NewWriter(ctx, WriterOptions{
		Compressor: "faulty",
	})

	_, err := w.Write(bytes.Repeat([]byte{1, 2, 3, 4}, 1e6))
	require.ErrorIs(t, err, errSomeError)
}
