package content

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/format"
)

func newTestingMapStorage() blob.Storage {
	data := blobtesting.DataMap{}
	keyTime := map[blob.ID]time.Time{}

	return blobtesting.NewMapStorage(data, keyTime, nil)
}

// newTestWriteManager is a helper to create a WriteManager for testing.
func newTestWriteManager(t *testing.T, st blob.Storage) *WriteManager {
	t.Helper()

	fp := mustCreateFormatProvider(t, &format.ContentFormat{
		Hash:       "HMAC-SHA256-128",
		Encryption: "AES256-GCM-HMAC-SHA256",
		HMACSecret: []byte("test-hmac"),
		MasterKey:  []byte("0123456789abcdef0123456789abcdef"),
		MutableParameters: format.MutableParameters{
			Version:         2,
			EpochParameters: epoch.DefaultParameters(),
			IndexVersion:    index.Version2,
			MaxPackSize:     1024 * 1024, // 1 MB
		},
	})

	bm, err := NewManagerForTesting(testlogging.Context(t), st, fp, nil, nil)

	require.NoError(t, err, "cannot create content write manager")

	return bm
}

func TestVerifyContents_NoMissingPacks(t *testing.T) {
	st := newTestingMapStorage()
	bm := newTestWriteManager(t, st)
	ctx := testlogging.Context(t)

	// Create pack by writing contents.
	_, err := bm.WriteContent(ctx, gather.FromSlice([]byte("hello")), "", NoCompression)
	require.NoError(t, err)

	_, err = bm.WriteContent(ctx, gather.FromSlice([]byte("hello prefixed")), "k", NoCompression)
	require.NoError(t, err)

	require.NoError(t, bm.Flush(ctx))

	err = bm.VerifyContents(ctx, VerifyOptions{
		ContentIterateParallelism: 1,
	})

	require.NoError(t, err, "verification should pass as the packs exists")
}

func TestVerifyContentToPackMapping_EnsureCallbackIsCalled(t *testing.T) {
	const numberOfContents = 6

	st := newTestingMapStorage()
	bm := newTestWriteManager(t, st)
	ctx := testlogging.Context(t)

	// Create numberOfContents contents
	var buf [4]byte

	for i := range numberOfContents {
		binary.LittleEndian.PutUint32(buf[:], uint32(i))
		_, err := bm.WriteContent(ctx, gather.FromSlice(buf[:]), "", NoCompression)
		require.NoError(t, err)
	}

	require.NoError(t, bm.Flush(ctx))

	var callbackCount atomic.Uint32 // use atomic to support higher parallelism

	cb := func(st VerifyProgressStats) {
		callbackCount.Add(1)
	}

	// verify that the callback is called twice (every numberOfContents / 2)
	err := bm.VerifyContents(ctx, VerifyOptions{
		ContentIterateParallelism: 1,
		ProgressCallback:          cb,
		ProgressCallbackInterval:  numberOfContents / 2,
	})

	require.NoError(t, err, "verification should pass as the packs exists")
	require.EqualValues(t, 2, callbackCount.Load(), "unexpected callback call count")

	// Delete the pack from storage so verification fails
	blobs, err := blob.ListAllBlobs(ctx, st, PackBlobIDPrefixRegular)
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.NoError(t, st.DeleteBlob(ctx, blobs[0].BlobID))

	callbackCount.Store(0)

	// verify the callback is called when there are errors as well.
	// verify that the callback is called twice (every numberOfContents / 2)
	err = bm.VerifyContents(ctx, VerifyOptions{
		ContentIterateParallelism: 1,
		ProgressCallback:          cb,
		ProgressCallbackInterval:  numberOfContents / 2,
	})

	require.Error(t, err, "verification should fail as the pack is missing")
	require.EqualValues(t, 2, callbackCount.Load(), "unexpected callback call count")
}

func TestVerifyContents_Deleted(t *testing.T) {
	st := newTestingMapStorage()
	bm := newTestWriteManager(t, st)
	ctx := testlogging.Context(t)

	// Create pack by writing contents.
	cid, err := bm.WriteContent(ctx, gather.FromSlice([]byte("hello 1")), "", NoCompression)

	require.NoError(t, err)
	require.NoError(t, bm.Flush(ctx))

	// get pack id
	blobs, err := blob.ListAllBlobs(ctx, st, PackBlobIDPrefixRegular)
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	packId := blobs[0].BlobID

	// write another content and delete the first content
	_, err = bm.WriteContent(ctx, gather.FromSlice([]byte("hello 2")), "", NoCompression)
	require.NoError(t, err)

	err = bm.DeleteContent(ctx, cid)
	require.NoError(t, err)

	require.NoError(t, bm.Flush(ctx))

	err = bm.VerifyContents(ctx, VerifyOptions{
		IncludeDeletedContents: true,
	})
	require.NoError(t, err, "Verification should succeed")

	// Delete the first pack from storage so verification fails
	require.NoError(t, st.DeleteBlob(ctx, packId))

	err = bm.VerifyContents(ctx, VerifyOptions{
		IncludeDeletedContents: false,
	})
	require.NoError(t, err, "Verification should succeed")

	err = bm.VerifyContents(ctx, VerifyOptions{
		IncludeDeletedContents: true,
	})
	require.Error(t, err, "Verification should fail when deleted contents are included and the pack for the deleted content is missing")
}

func TestVerifyContents_TruncatedPack(t *testing.T) {
	st := newTestingMapStorage()
	bm := newTestWriteManager(t, st)
	ctx := testlogging.Context(t)

	// Create pack by writing contents.
	_, err := bm.WriteContent(ctx, gather.FromSlice([]byte("hello")), "", NoCompression)
	require.NoError(t, err)

	_, err = bm.WriteContent(ctx, gather.FromSlice([]byte("hello prefixed")), "k", NoCompression)
	require.NoError(t, err)

	require.NoError(t, bm.Flush(ctx))

	// Truncate the pack so verification fails
	blobs, err := blob.ListAllBlobs(ctx, st, PackBlobIDPrefixRegular)
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.NoError(t, st.PutBlob(ctx, blobs[0].BlobID, gather.Bytes{}, blob.PutOptions{}))

	err = bm.VerifyContents(ctx, VerifyOptions{})
	require.Error(t, err, "Verification should fail when a 'p' pack blob is truncated")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllNonPrefixedIDs})
	require.Error(t, err, "Verification should fail when a 'p' pack blob is truncated and non-prefixed contents are verified")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllPrefixedIDs})
	require.NoError(t, err, "verification should succeed when a 'p' pack blob is truncated and prefixed contents are verified")
}

func TestVerifyContents_CorruptedPack(t *testing.T) {
	st := newTestingMapStorage()
	bm := newTestWriteManager(t, st)
	ctx := testlogging.Context(t)

	// Create pack by writing contents.
	_, err := bm.WriteContent(ctx, gather.FromSlice([]byte("hello")), "", NoCompression)
	require.NoError(t, err)

	_, err = bm.WriteContent(ctx, gather.FromSlice([]byte("hello prefixed")), "k", NoCompression)
	require.NoError(t, err)

	require.NoError(t, bm.Flush(ctx))

	// Corrupt the pack so verification fails
	blobs, err := blob.ListAllBlobs(ctx, st, PackBlobIDPrefixRegular)
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	bid := blobs[0].BlobID

	meta, err := st.GetMetadata(ctx, bid)
	require.NoError(t, err)
	require.NotZero(t, meta)

	bSize := meta.Length
	require.NotZero(t, bSize)

	err = st.PutBlob(ctx, bid, gather.FromSlice(bytes.Repeat([]byte{1}, int(bSize))), blob.PutOptions{})
	require.NoError(t, err)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentReadPercentage: 100})
	require.Error(t, err, "Verification should fail when a 'p' pack blob is corrupted")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllNonPrefixedIDs, ContentReadPercentage: 100})
	require.Error(t, err, "Verification should fail when a 'p' pack blob is corrupted and non-prefixed contents are verified")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllPrefixedIDs, ContentReadPercentage: 100})
	require.NoError(t, err, "verification should succeed when a 'p' pack blob is corrupted and prefixed contents are verified")
}

func TestVerifyContents_MissingPackP(t *testing.T) {
	st := newTestingMapStorage()
	bm := newTestWriteManager(t, st)
	ctx := testlogging.Context(t)

	// Create pack by writing contents.
	_, err := bm.WriteContent(ctx, gather.FromSlice([]byte("hello")), "", NoCompression)
	require.NoError(t, err)

	_, err = bm.WriteContent(ctx, gather.FromSlice([]byte("hello prefixed")), "k", NoCompression)
	require.NoError(t, err)

	require.NoError(t, bm.Flush(ctx))

	// Delete pack so verification fails
	blobs, err := blob.ListAllBlobs(ctx, st, PackBlobIDPrefixRegular)
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.NoError(t, st.DeleteBlob(ctx, blobs[0].BlobID))

	err = bm.VerifyContents(ctx, VerifyOptions{})
	require.Error(t, err, "Verification should fail when a 'p' pack blob is missing")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllNonPrefixedIDs})
	require.Error(t, err, "Verification should fail when a 'p' pack blob is missing and non-prefixed contents are verified")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllPrefixedIDs})
	require.NoError(t, err, "verification should succeed when a 'p' pack blob is missing and prefixed contents are verified")
}

func TestVerifyContentToPackMapping_MissingPackQ(t *testing.T) {
	st := newTestingMapStorage()
	bm := newTestWriteManager(t, st)
	ctx := testlogging.Context(t)

	// Create a 'p' pack by writing a non-prefixed content
	_, err := bm.WriteContent(ctx, gather.FromSlice([]byte("hello")), "", NoCompression)
	require.NoError(t, err)

	// Create a 'q' pack by writing a prefixed content
	_, err = bm.WriteContent(ctx, gather.FromSlice([]byte("hello prefixed")), "k", NoCompression)
	require.NoError(t, err)

	require.NoError(t, bm.Flush(ctx))

	// Delete the pack with 'q' prefix so verification fails
	blobs, err := blob.ListAllBlobs(ctx, st, PackBlobIDPrefixSpecial)
	require.NoError(t, err)
	require.Len(t, blobs, 1)
	require.NoError(t, st.DeleteBlob(ctx, blobs[0].BlobID))

	err = bm.VerifyContents(ctx, VerifyOptions{})
	require.Error(t, err, "verification should fail when a 'q' pack blob is missing")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllPrefixedIDs})
	require.Error(t, err, "verification should fail when a 'q' pack blob is missing and prefixed contents are verified")
	require.ErrorIs(t, err, errMissingPacks)

	err = bm.VerifyContents(ctx, VerifyOptions{ContentIDRange: index.AllNonPrefixedIDs})
	require.NoError(t, err, "verification should succeed when a 'q' pack blob is missing and non-prefixed contents are verified")
}
