//go:build !release

package storj

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"storj.io/uplink"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

var (
	errExpected           = errors.New("expected error")
	errPuttingBlob        = errors.New("error putting blob")
	errUnexpectedRead     = errors.New("unexpected error when reading partial blob")
	errUnexpectedReadData = errors.New("unexpected data after reading partial blob")
)

func randBlobID(_ *testing.T) string {
	seededRnd := rand.New(rand.NewSource(clock.Now().UnixNano()))

	const (
		charset = "0123456789abcdef"
		length  = 36
	)

	rs := make([]byte, length)
	for i := range length {
		rs[i] = charset[seededRnd.Intn(len(charset)-1)]
	}

	return string(rs)
}

func randBytes(t *testing.T, length int) []byte {
	t.Helper()

	rndcont := make([]byte, length)
	_, err := crand.Read(rndcont)
	require.NoError(t, err)

	return rndcont
}

// utility to directly delete a bucket without instantiating StorjStorage.
func deleteBucket(ctx context.Context, opt *Options, bucket string) error {
	return UlDeleteBucket(ctx, bucket, opt.KeyOrGrant)
}

// The (most) important tests we need to actually verify correct functioning of PutBlob and GetBlob
// are the ones at the interface between kopia and the backend!
// I.e. we should emulate the file <-> buffer interaction in the test, and directly execute GetBlob and PutBlob and compare the outcome separately with independent file(=blob) comparison
// This is actually already provided by blobtesting/verify.go, or in other words we should just craft tests similar to how it's done in the s3 backend

// This is a modification of blobtesting.VerifyStorage that allows us to select specific tests.
//
//nolint:gocyclo,maintidx
func VerifyStorjage(ctx context.Context, t *testing.T, r blob.Storage, opts blob.PutOptions, selectTests []string) {
	t.Helper()

	blocks := []struct {
		blk      blob.ID
		contents []byte
	}{
		{blk: "abcdbbf4f0507d054ed5a80a5b65086f602b", contents: /* bytes.Repeat([]byte{5}, 10000)}, */ []byte{}},
		{blk: "zxce0e35630770c54668a8cfb4e414c6bf8f", contents: /* bytes.Repeat([]byte{2}, 10000)}, */ []byte{1}},
		{blk: "abff4585856ebf0748fd989e1dd623a8963d", contents: /* bytes.Repeat([]byte{1}, 10000)}, */ bytes.Repeat([]byte{1}, 1000)},
		{blk: "abgc3dca496d510f492c858a2df1eb824e62", contents: bytes.Repeat([]byte{3}, 10000)},
		{blk: "kopia.repository", contents: bytes.Repeat([]byte{2}, 100)},
	}

	// First verify that blocks don't exist.
	if slices.Contains(selectTests, "VerifyBlobsNotFound") {
		t.Run("VerifyBlobsNotFound", func(t *testing.T) {
			for _, b := range blocks {
				t.Run(string(b.blk), func(t *testing.T) {
					t.Parallel()

					blobtesting.AssertGetBlobNotFound(ctx, t, r, b.blk)
					blobtesting.AssertGetMetadataNotFound(ctx, t, r, b.blk)
				})
			}
		})

		if err := r.DeleteBlob(ctx, "no-such-blob"); err != nil && !errors.Is(err, blob.ErrBlobNotFound) {
			t.Errorf("invalid error when deleting non-existent blob: %v", err)
		}
	}

	initialAddConcurrency := 2
	if os.Getenv("CI") != "" {
		initialAddConcurrency = 4
	}

	// Now add blocks.
	// we should do this unconditionally as prerequisite for subsequent tests?
	doit := false
	for _, s := range []string{"AddBlobs", "GetBlobs", "ListBlobs", "OverwriteBlobs", "ExtendBlobRetention", "DeleteBlobsAndList"} {
		if slices.Contains(selectTests, s) {
			doit = true
			break
		}
	}

	if doit {
		t.Run("AddBlobs", func(t *testing.T) {
			for _, b := range blocks {
				for i := range initialAddConcurrency {
					b := b

					t.Run(fmt.Sprintf("%v-%v", b.blk, i), func(t *testing.T) {
						// t.Parallel()
						if err := r.PutBlob(ctx, b.blk, gather.FromSlice(b.contents), opts); err != nil {
							t.Fatalf("can't put blob: %v", err)
						}
					})
				}
			}
		})
	}

	if slices.Contains(selectTests, "GetBlobs") {
		t.Run("GetBlobs", func(t *testing.T) {
			for _, b := range blocks {
				t.Run(string(b.blk), func(t *testing.T) {
					// t.Parallel()
					blobtesting.AssertGetBlob(ctx, t, r, b.blk, b.contents)
				})
			}
		})
	}

	if slices.Contains(selectTests, "ListBlobs") {
		t.Run("ListBlobs", func(t *testing.T) {
			t.Run("ListErrorNoPrefix", func(t *testing.T) {
				t.Parallel()
				require.ErrorIs(t, r.ListBlobs(ctx, "", func(bm blob.Metadata) error {
					return errExpected
				}), errExpected)
			})
			t.Run("ListErrorWithPrefix", func(t *testing.T) {
				t.Parallel()
				require.ErrorIs(t, r.ListBlobs(ctx, "ab", func(bm blob.Metadata) error {
					return errExpected
				}), errExpected)
			})
			t.Run("ListNoPrefix", func(t *testing.T) {
				t.Parallel()
				blobtesting.AssertListResults(ctx, t, r, "", blocks[0].blk, blocks[1].blk, blocks[2].blk, blocks[3].blk, blocks[4].blk)
			})
			t.Run("ListWithPrefix", func(t *testing.T) {
				t.Parallel()
				blobtesting.AssertListResults(ctx, t, r, "ab", blocks[0].blk, blocks[2].blk, blocks[3].blk)
			})
		})
	}

	if slices.Contains(selectTests, "OverwriteBlobs") {
		t.Run("OverwriteBlobs", func(t *testing.T) {
			newContents := []byte{99}

			for _, b := range blocks {
				t.Run(string(b.blk), func(t *testing.T) {
					t.Parallel()

					err := r.PutBlob(ctx, b.blk, gather.FromSlice(newContents), opts)
					if opts.DoNotRecreate {
						require.ErrorIsf(t, err, blob.ErrBlobAlreadyExists, "overwrote blob: %v", b)
						blobtesting.AssertGetBlob(ctx, t, r, b.blk, b.contents)
					} else {
						require.NoErrorf(t, err, "can't put blob: %v", b)
						blobtesting.AssertGetBlob(ctx, t, r, b.blk, newContents)
					}
				})
			}
		})
	}

	if slices.Contains(selectTests, "ExtendBlobRetention") {
		t.Run("ExtendBlobRetention", func(t *testing.T) {
			err := r.ExtendBlobRetention(ctx, blocks[0].blk, blob.ExtendOptions{
				RetentionMode:   opts.RetentionMode,
				RetentionPeriod: opts.RetentionPeriod,
			})
			if opts.RetentionMode != "" && err != nil {
				t.Fatalf("No error expected during extend retention: %v", err)
			} else if opts.RetentionMode == "" && err == nil {
				t.Fatal("No error found when expected during extend retention")
			}
		})
	}

	if slices.Contains(selectTests, "DeleteBlobsAndList") {
		t.Run("DeleteBlobsAndList", func(t *testing.T) {
			require.NoError(t, r.DeleteBlob(ctx, blocks[0].blk))
			require.NoError(t, r.DeleteBlob(ctx, blocks[0].blk))

			blobtesting.AssertListResults(ctx, t, r, "ab", blocks[2].blk, blocks[3].blk)
			blobtesting.AssertListResults(ctx, t, r, "", blocks[1].blk, blocks[2].blk, blocks[3].blk, blocks[4].blk)
		})
	}

	if slices.Contains(selectTests, "PutBlobsWithSetTime") {
		t.Run("PutBlobsWithSetTime", func(t *testing.T) {
			for _, b := range blocks {
				t.Run(string(b.blk), func(t *testing.T) {
					t.Parallel()

					inTime := time.Date(2020, 1, 2, 12, 30, 40, 0, time.UTC)

					err := r.PutBlob(ctx, b.blk, gather.FromSlice(b.contents), blob.PutOptions{
						SetModTime: inTime,
					})

					if errors.Is(err, blob.ErrSetTimeUnsupported) {
						t.Skip("setting time unsupported")
					}

					bm, err := r.GetMetadata(ctx, b.blk)
					require.NoError(t, err)

					blobtesting.AssertTimestampsCloseEnough(t, bm.BlobID, bm.Timestamp, inTime)

					all, err := blob.ListAllBlobs(ctx, r, b.blk)
					require.NoError(t, err)
					require.Len(t, all, 1)

					blobtesting.AssertTimestampsCloseEnough(t, all[0].BlobID, all[0].Timestamp, inTime)
				})
			}
		})
	}

	if slices.Contains(selectTests, "PutBlobsWithGetTime") {
		t.Run("PutBlobsWithGetTime", func(t *testing.T) {
			for _, b := range blocks {
				t.Run(string(b.blk), func(t *testing.T) {
					t.Parallel()

					var outTime time.Time

					require.NoError(t, r.PutBlob(ctx, b.blk, gather.FromSlice(b.contents), blob.PutOptions{
						GetModTime: &outTime,
					}))

					require.False(t, outTime.IsZero(), "modification time was not returned")

					bm, err := r.GetMetadata(ctx, b.blk)
					require.NoError(t, err)

					blobtesting.AssertTimestampsCloseEnough(t, bm.BlobID, bm.Timestamp, outTime)

					all, err := blob.ListAllBlobs(ctx, r, b.blk)
					require.NoError(t, err)
					require.Len(t, all, 1)

					blobtesting.AssertTimestampsCloseEnough(t, all[0].BlobID, all[0].Timestamp, outTime)
				})
			}
		})
	}
}

func TestStorjage(t *testing.T) {
	const testName string = "TestStorjage"

	ctx := context.Background()
	repoOpt := GetUplinkCreds()

	// cleanup in case of aborted/failed previous runs
	err := ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	storage, err := New(ctx, repoOpt, true)
	if err != nil {
		t.Fatalf("%s: %s", testName, err.Error())
	}

	defer func() { _ = storage.Close(ctx) }()

	putOptions := blob.PutOptions{}

	dotests := []string{
		"VerifyBlobsNotFound",
		"AddBlobs",
		// "GetBlobs",
		// "ListBlobs",
		// "OverwriteBlobs",
		// "ExtendBlobRetention",
		"DeleteBlobsAndList",
		// "PutBlobsWithSetTime",
		// "PutBlobsWithGetTime",
	}
	// here the magic happens
	VerifyStorjage(ctx, t, storage, putOptions, dotests)

	err = ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}
}

func TestStorjStorage(t *testing.T) {
	const testName string = "TestStorjStorage"

	ctx := context.Background()
	repoOpt := GetUplinkCreds()

	// cleanup in case of aborted/failed previous runs
	err := ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	storage, err := New(ctx, repoOpt, true)
	if err != nil {
		t.Fatalf("%s: %s", testName, err.Error())
	}

	defer func() { _ = storage.Close(ctx) }()

	putOptions := blob.PutOptions{}

	// here the magic happens
	blobtesting.VerifyStorage(ctx, t, storage, putOptions)

	err = deleteBucket(ctx, repoOpt, repoOpt.BucketName)
	require.ErrorIsf(t, err, uplink.ErrBucketNotEmpty, "%s", err.Error())

	err = ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}
}

// for development of PutBlob and GetBlob, later covered by TestStorjStorage.
func TestBasicPutGetBlob(t *testing.T) {
	const testName string = "TestBasicPutGetBlob"

	ctx := context.Background()
	repoOpt := GetUplinkCreds()

	storage, err := New(ctx, repoOpt, true)
	if err != nil {
		t.Fatalf("%s: %s", testName, err.Error())
	}

	// since a "blob" is just a remote file object, we might as well just upload a recognizable file,
	// so we can also manually check
	oname := "storj_storage_test.go"

	buf, err := os.ReadFile(oname)
	if err != nil {
		t.Fatalf("%s: %s", testName, err.Error())
	}

	putOptions := blob.PutOptions{}

	err = storage.PutBlob(ctx, blob.ID(oname), gather.FromSlice(buf), putOptions)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	rbuf := gather.NewWriteBuffer()

	err = storage.GetBlob(ctx, blob.ID(oname), 0, -1, rbuf)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	assert.Equal(t, rbuf.Bytes().ToByteSlice(), buf) // compare written with read object

	md, err := storage.GetMetadata(ctx, blob.ID(oname))
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	t.Logf("got metadata for %q: %#v", oname, md)
	assert.Equal(t, blob.ID(oname), md.BlobID)
	assert.Equal(t, int64(len(buf)), md.Length)
	assert.Less(t, clock.Now().UTC().Sub(md.Timestamp.UTC()), time.Minute) // object create timestamp is within one minute from this test execution

	// cleanup
	err = storage.DeleteBlob(ctx, blob.ID(oname))
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}
}

func TestMultiPutGetBlob(t *testing.T) {
	type block struct {
		blk      blob.ID
		contents []byte
	}

	ctx := context.Background()

	repoOpt := GetUplinkCreds()
	storage, err := New(ctx, repoOpt, true)
	require.NoError(t, err)

	blocks := make([]block, 0, 6)
	defer func(t *testing.T) {
		t.Helper()

		// cleanup
		for _, b := range blocks {
			err = storage.DeleteBlob(ctx, b.blk)
			assert.NoError(t, err)
		}
	}(t)

	for i := 10; i <= 20; i += 2 {
		b := block{
			blk:      blob.ID(randBlobID(t)),
			contents: randBytes(t, 1<<i),
		}
		t.Logf("made block: %s (%8d bytes)", b.blk, len(b.contents))
		blocks = append(blocks, b)
	}

	putOptions := blob.PutOptions{}

	// put all blocks
	for _, b := range blocks {
		err = storage.PutBlob(ctx, b.blk, gather.FromSlice(b.contents), putOptions)
		require.NoError(t, err)
	}

	// get and compare all blocks
	for _, b := range blocks {
		rbuf := gather.NewWriteBuffer()
		err = storage.GetBlob(ctx, b.blk, 0, -1, rbuf)
		require.NoError(t, err)
		assert.Equal(t, rbuf.Bytes().ToByteSlice(), b.contents) // compare written with read object
		// additionally check the metadata:
		md, err := storage.GetMetadata(ctx, b.blk)
		require.NoError(t, err)

		if assert.Equalf(t, b.blk, md.BlobID, "source object blobID %q and metadata blobID %q do not match", b.blk, md.BlobID) {
			t.Logf("OK: source object blobID %q and metadata blobID match", b.blk)
		}

		if assert.Equalf(t, int64(len(b.contents)), md.Length, "source object content length %8d and metadata content length %8d bytes do not match", len(b.contents), md.Length) {
			t.Logf("OK: source object content length %8d and metadata content length match", len(b.contents))
		}

		if assert.Lessf(t, clock.Now().UTC().Sub(md.Timestamp.UTC()), time.Minute, "current timestamp %v and metadata timestamp %v deviation too large (> 1 min)", clock.Now().UTC(), md.Timestamp.UTC()) {
			t.Logf("OK: metadata timestamp (%s) and current time are near enough (less than 1 min)", md.Timestamp.UTC())
		}
	}
}

// interface.
func TestNew(t *testing.T) {
	const testName = "TestNew"

	ctx := context.Background()

	repoOpt := GetUplinkCreds()

	_, err := New(ctx, repoOpt, true)
	if err != nil {
		t.Error(testName)
	}
}

// function tests
// NOTE: for now we turn off bucket level operations, since we limit access to one test bucket storj account
// func TestCreateDeleteBucket(t *testing.T) {
// 	testName := "TestCreateDeleteBucket"
// 	ctx := context.Background()
//
// 	repoOpt := GetUplinkCreds()
// 	repoOpt.BucketName = "create-delete"
//
// 	_, err := ensureBucket(ctx, repoOpt, repoOpt.BucketName)
// 	if err != nil {
// 		t.Errorf("%s: %s", testName, err.Error())
// 	}
//
// 	_, err = createBucket(ctx, repoOpt, repoOpt.BucketName)
// 	assert.ErrorIs(t, err, uplink.ErrBucketAlreadyExists)
//
// 	err = deleteBucket(ctx, repoOpt, repoOpt.BucketName)
// 	if err != nil {
// 		t.Errorf("%s: %s", testName, err.Error())
// 	}
//
// }
//
// func TestDeleteBucketNotExist(t *testing.T) {
// 	ctx := context.Background()
//
// 	repoOpt := GetUplinkCreds()
// 	repoOpt.BucketName = "bucket-notexist"
//
// 	err := deleteBucket(ctx, repoOpt, repoOpt.BucketName)
// 	assert.ErrorIs(t, err, uplink.ErrBucketNotFound)
// }

func TestPutBlob(t *testing.T) {
	testName := "TestPutBlob"
	ctx := context.Background()

	repoOpt := GetUplinkCreds()

	err := ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	storjStorage, err := New(ctx, repoOpt, false) // true: create bucket if not exists
	if err != nil {
		t.Fatalf("%s: %s", testName, err.Error())
	}

	putOptions := blob.PutOptions{}
	buf := make([]byte, 128)

	err = storjStorage.PutBlob(ctx, blob.ID(testId), gather.FromSlice(buf), putOptions)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}
}

func TestPutKopiaBlobs(t *testing.T) {
	const testName = "TestPutBlob"

	kbs := []struct {
		blk      blob.ID
		contents []byte
	}{
		{blk: "kopia.repository-0", contents: bytes.Repeat([]byte{2}, 100)},
		{blk: "kopia.repository-1", contents: bytes.Repeat([]byte{4}, 100)},
		{blk: "kopia.repository-2", contents: bytes.Repeat([]byte{6}, 100)},
	}
	ctx := context.Background()

	repoOpt := GetUplinkCreds()

	err := ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	storjStorage, err := New(ctx, repoOpt, false) // true: create bucket if not exists
	if err != nil {
		t.Fatalf("%s: %s", testName, err.Error())
	}

	putOptions := blob.PutOptions{}

	for _, b := range kbs {
		err = storjStorage.PutBlob(ctx, b.blk, gather.FromSlice(b.contents), putOptions)
		if err != nil {
			err = errors.Join(err, fmt.Errorf("%q: %w", b.blk, errPuttingBlob))
		}
	}

	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}
}

func TestBlobPartialRead(t *testing.T) {
	testName := "TestBlobPartialRead"
	ctx := context.Background()

	repoOpt := GetUplinkCreds()

	err := ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	storjStorage, err := New(ctx, repoOpt, false)
	if err != nil {
		t.Fatalf("%s: %s", testName, err.Error())
	}

	putOptions := blob.PutOptions{}

	buf := randBytes(t, 1e6)

	err = storjStorage.PutBlob(ctx, blob.ID(testId), gather.FromSlice(buf), putOptions)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}

	var out gather.WriteBuffer
	defer out.Close()

	partialBlobCases := []struct {
		offset int64
		length int64
	}{
		{0, 10},
		{1, 10},
		{2, 1},
		{5, 0},
		{int64(len(buf)) - 5, 5},
	}

	var err1, err2, err3 error

	for _, tc := range partialBlobCases {
		err1 = storjStorage.GetBlob(ctx, blob.ID(testId), tc.offset, tc.length, &out)
		if err != nil {
			err2 = errors.Join(err1, fmt.Errorf("@%v+%v: %w", tc.offset, tc.length, errUnexpectedRead))
		}

		require.NoError(t, err2)

		if got, want := out.ToByteSlice(), buf[tc.offset:tc.offset+tc.length]; !bytes.Equal(got, want) {
			err3 = errors.Join(err, fmt.Errorf(`@%v+%v: got "%x", wanted "%x": %w`, tc.offset, tc.length, got, want, errUnexpectedReadData))
		}

		require.NoError(t, err3)
	}

	err = ULDeleteAllObjects(ctx, repoOpt.BucketName, repoOpt.KeyOrGrant)
	if err != nil {
		t.Errorf("%s: %s", testName, err.Error())
	}
}
