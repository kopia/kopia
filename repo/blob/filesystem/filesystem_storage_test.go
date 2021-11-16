package filesystem

import (
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
	testutil.ProviderTest(t)

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

		r, err := New(ctx, &Options{
			Path: path,
			Options: sharded.Options{
				DirectoryShards: shardSpec,
			},
		}, true)

		if r == nil || err != nil {
			t.Errorf("unexpected result: %v %v", r, err)
		}

		blobtesting.VerifyStorage(ctx, t, r, blob.PutOptions{})
		blobtesting.AssertConnectionInfoRoundTrips(ctx, t, r)
		require.NoError(t, providervalidation.ValidateProvider(ctx, r, blobtesting.TestValidationOptions))

		if err := r.Close(ctx); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
}

const (
	t1 = "392ee1bc299db9f235e046a62625afb84902"
	t2 = "2a7ff4f29eddbcd4c18fa9e73fec20bbb71f"
	t3 = "0dae5918f83e6a24c8b3e274ca1026e43f24"
)

func TestFileStorageTouch(t *testing.T) {
	t.Parallel()
	testutil.ProviderTest(t)

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
	if err != nil {
		t.Fatal(err)
	}

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
	if err != nil {
		t.Fatalf("unable to connect to rclone backend: %v", err)
	}

	defer st.Close(ctx)

	require.NoError(t, st.PutBlob(ctx, "someblob1234567812345678", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	require.FileExists(t, filepath.Join(dataDir, "someb", "lo", "b1234567812345678.f"))
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
