package filesystem

import (
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/kopia/kopia/repo/blob"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
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
		path, _ := ioutil.TempDir("", "r-fs")
		defer os.RemoveAll(path)

		r, err := New(ctx, &Options{
			Path:            path,
			DirectoryShards: shardSpec,
		})

		if r == nil || err != nil {
			t.Errorf("unexpected result: %v %v", r, err)
		}

		blobtesting.VerifyStorage(ctx, t, r)
		blobtesting.AssertConnectionInfoRoundTrips(ctx, t, r)

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

	ctx := testlogging.Context(t)

	path, _ := ioutil.TempDir("", "r-fs")
	defer os.RemoveAll(path)

	r, err := New(ctx, &Options{
		Path: path,
	})

	if r == nil || err != nil {
		t.Errorf("unexpected result: %v %v", r, err)
	}

	fs := r.(*fsStorage)
	assertNoError(t, fs.PutBlob(ctx, t1, gather.FromSlice([]byte{1})))
	time.Sleep(1 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution
	assertNoError(t, fs.PutBlob(ctx, t2, gather.FromSlice([]byte{1})))
	time.Sleep(1 * time.Second)
	assertNoError(t, fs.PutBlob(ctx, t3, gather.FromSlice([]byte{1})))

	verifyBlobTimestampOrder(t, fs, t1, t2, t3)

	assertNoError(t, fs.TouchBlob(ctx, t2, 1*time.Hour)) // has no effect, all timestamps are very new
	verifyBlobTimestampOrder(t, fs, t1, t2, t3)

	assertNoError(t, fs.TouchBlob(ctx, t1, 0)) // moves t1 to the top of the pile
	verifyBlobTimestampOrder(t, fs, t2, t3, t1)
	time.Sleep(1 * time.Second)

	assertNoError(t, fs.TouchBlob(ctx, t2, 0)) // moves t2 to the top of the pile
	verifyBlobTimestampOrder(t, fs, t3, t1, t2)
	time.Sleep(1 * time.Second)

	assertNoError(t, fs.TouchBlob(ctx, t1, 0)) // moves t1 to the top of the pile
	verifyBlobTimestampOrder(t, fs, t3, t2, t1)
}

func TestFileStorageConcurrency(t *testing.T) {
	path, _ := ioutil.TempDir("", "fs-concurrency")
	defer os.RemoveAll(path)

	ctx := testlogging.Context(t)

	st, err := New(ctx, &Options{
		Path: path,
	})

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

func verifyBlobTimestampOrder(t *testing.T, st blob.Storage, want ...blob.ID) {
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
