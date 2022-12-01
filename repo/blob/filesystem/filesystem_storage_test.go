package filesystem

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/providervalidation"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/sharded"
)

func TestFileStorage(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	// Test varioush shard configurations.
	for _, shardSpec := range [][]int{
		{0},
		{1},
		{3, 3},
		{2},
		{1, 1},
		{1, 2},
		{2, 2, 2},
	} {
		path := testutil.TempDirectory(t)

		newctx, cancel := context.WithCancel(ctx)

		// use context that gets canceled after opening storage to ensure it's not used beyond New().
		r, err := New(newctx, &Options{
			Path: path,
			Options: sharded.Options{
				DirectoryShards: shardSpec,
			},
		}, true)

		cancel()
		require.NoError(t, err)
		require.NotNil(t, r)

		blobtesting.VerifyStorage(ctx, t, r, blob.PutOptions{})
		blobtesting.AssertConnectionInfoRoundTrips(ctx, t, r)

		require.NoError(t, r.Close(ctx))
	}
}

func TestFileStorageValidate(t *testing.T) {
	t.Parallel()

	testutil.ProviderTest(t)

	ctx := testlogging.Context(t)

	path := testutil.TempDirectory(t)

	r, err := New(ctx, &Options{
		Path:    path,
		Options: sharded.Options{},
	}, true)

	require.NoError(t, err)
	require.NotNil(t, r)

	blobtesting.VerifyStorage(ctx, t, r, blob.PutOptions{})
	blobtesting.AssertConnectionInfoRoundTrips(ctx, t, r)
	require.NoError(t, providervalidation.ValidateProvider(ctx, r, blobtesting.TestValidationOptions))

	require.NoError(t, r.Close(ctx))
}

const (
	t1 = "392ee1bc299db9f235e046a62625afb84902"
	t2 = "2a7ff4f29eddbcd4c18fa9e73fec20bbb71f"
	t3 = "0dae5918f83e6a24c8b3e274ca1026e43f24"
)

func TestFileStorageTouch(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	path := testutil.TempDirectory(t)

	r, err := New(ctx, &Options{
		Path: path,
	}, true)

	if r == nil || err != nil {
		t.Errorf("unexpected result: %v %v", r, err)
	}

	fs := r.(*fsStorage)
	assertNoError(t, fs.PutBlob(ctx, t1, gather.FromSlice([]byte{1}), blob.PutOptions{}))
	time.Sleep(2 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution
	assertNoError(t, fs.PutBlob(ctx, t2, gather.FromSlice([]byte{1}), blob.PutOptions{}))
	time.Sleep(2 * time.Second)
	assertNoError(t, fs.PutBlob(ctx, t3, gather.FromSlice([]byte{1}), blob.PutOptions{}))
	time.Sleep(2 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution

	verifyBlobTimestampOrder(t, fs, t1, t2, t3)

	assertNoError(t, fs.TouchBlob(ctx, t2, 1*time.Hour)) // has no effect, all timestamps are very new
	verifyBlobTimestampOrder(t, fs, t1, t2, t3)
	time.Sleep(2 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution

	assertNoError(t, fs.TouchBlob(ctx, t1, 0)) // moves t1 to the top of the pile
	verifyBlobTimestampOrder(t, fs, t2, t3, t1)
	time.Sleep(2 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution

	assertNoError(t, fs.TouchBlob(ctx, t2, 0)) // moves t2 to the top of the pile
	verifyBlobTimestampOrder(t, fs, t3, t1, t2)
	time.Sleep(2 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution

	assertNoError(t, fs.TouchBlob(ctx, t1, 0)) // moves t1 to the top of the pile
	verifyBlobTimestampOrder(t, fs, t3, t2, t1)
}

func TestFileStorageConcurrency(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

	path := testutil.TempDirectory(t)

	ctx := testlogging.Context(t)

	st, err := New(ctx, &Options{
		Path: path,
	}, true)
	require.NoError(t, err)

	blobtesting.VerifyConcurrentAccess(t, st, blobtesting.ConcurrentAccessOptions{
		NumBlobs:                        16,
		Getters:                         8,
		Putters:                         8,
		Deleters:                        8,
		Listers:                         8,
		Iterations:                      500,
		RangeGetPercentage:              10,
		NonExistentListPrefixPercentage: 10,
	})
}

func TestFilesystemStorageDirectoryShards(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	require.FileExists(t, filepath.Join(dataDir, "someb", "lo", "b1234567812345678.f"))
}

func TestFileStorage_GetBlob_RetriesOnReadError(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	osi.readFileRemainingErrors.Store(1)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
		osInterfaceOverride: osi,
	}, true)
	require.NoError(t, err)

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	var buf gather.WriteBuffer
	defer buf.Close()

	require.NoError(t, st.GetBlob(ctx, "someblob1234567812345678", 1, 2, &buf))
	require.Equal(t, []byte{2, 3}, buf.ToByteSlice())
}

func TestFileStorage_GetMetadata_RetriesOnError(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	osi.statRemainingErrors.Store(1)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	st.(*fsStorage).Impl.(*fsImpl).osi = osi

	_, err = st.GetMetadata(ctx, "someblob1234567812345678")
	require.NoError(t, err)
}

func TestFileStorage_PutBlob_RetriesOnErrors(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	osi.createNewFileRemainingErrors.Store(3)
	osi.mkdirAllRemainingErrors.Store(2)
	osi.writeFileRemainingErrors.Store(3)
	osi.writeFileCloseRemainingErrors.Store(2)
	osi.renameRemainingErrors.Store(1)
	osi.removeRemainingRetriableErrors.Store(3)
	osi.chownRemainingErrors.Store(3)
	osi.chtimesRemainingErrors.Store(3)

	fileUID := 3
	fileGID := 4

	st, err := New(ctx, &Options{
		Path:    dataDir,
		FileUID: &fileUID,
		FileGID: &fileGID,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	st.(*fsStorage).Impl.(*fsImpl).osi = osi

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	var buf gather.WriteBuffer
	defer buf.Close()

	require.NoError(t, st.GetBlob(ctx, "someblob1234567812345678", 1, 2, &buf))
	require.Equal(t, []byte{2, 3}, buf.ToByteSlice())

	var mt time.Time

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{
		GetModTime: &mt,
	}))

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{
		SetModTime: time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC),
	}))
}

func TestFileStorage_DeleteBlob_ErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()
	osi.removeRemainingNonRetriableErrors.Store(1)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	st.(*fsStorage).Impl.(*fsImpl).osi = osi

	defer st.Close(ctx)

	require.ErrorIs(t, st.DeleteBlob(ctx, "someblob1234567812345678"), errNonRetriable)
}

func TestFileStorage_New_MkdirAllFailureIsIgnored(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()
	osi.mkdirAllRemainingErrors.Store(1)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
		osInterfaceOverride: osi,
	}, true)
	require.NoError(t, err)

	defer st.Close(ctx)
}

func TestFileStorage_New_ChecksDirectoryExistence(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	osi.statRemainingErrors.Store(1)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
		osInterfaceOverride: osi,
	}, true)
	require.Error(t, err)
	require.Nil(t, st)
}

func TestFileStorage_ListBlobs_ErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	osi.readDirRemainingErrors.Store(3)
	osi.readDirRemainingFileDeletedDirEntry.Store(3)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	st.(*fsStorage).Impl.(*fsImpl).osi = osi

	defer st.Close(ctx)

	require.NoError(t, st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return nil
	}))

	osi.readDirRemainingNonRetriableErrors.Store(1)

	require.ErrorIs(t, st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return nil
	}), errNonRetriable)

	osi.readDirRemainingFatalDirEntry.Store(1)

	require.ErrorIs(t, st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		return nil
	}), errNonRetriable)
}

func TestFileStorage_TouchBlob_ErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	osi := newMockOS()

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	st.(*fsStorage).Impl.(*fsImpl).osi = osi

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))

	osi.statRemainingErrors.Store(1)

	require.NoError(t, st.(*fsStorage).TouchBlob(ctx, "someblob1234567812345678", 0))
}

func TestFileStorage_Misc(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	dataDir := testutil.TempDirectory(t)

	st, err := New(ctx, &Options{
		Path: dataDir,
		Options: sharded.Options{
			DirectoryShards: []int{5, 2},
		},
	}, true)
	require.NoError(t, err)

	defer st.Close(ctx)

	require.NoError(t, st.FlushCaches(ctx)) // this does nothing
	require.Equal(t, st.DisplayName(), "Filesystem: "+dataDir)
}

func verifyBlobTimestampOrder(t *testing.T, st blob.Storage, want ...blob.ID) {
	t.Helper()

	blobs, err := blob.ListAllBlobs(testlogging.Context(t), st, "")
	if err != nil {
		t.Errorf("error listing blobs: %v", err)
		return
	}

	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].Timestamp.Before(blobs[j].Timestamp)
	})

	var got []blob.ID
	for _, b := range blobs {
		got = append(got, b.BlobID)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect blob order: %v, wanted %v", blobs, want)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("err: %v", err)
	}
}

func newMockOS() *mockOS {
	return &mockOS{
		osInterface: realOS{},
	}
}
