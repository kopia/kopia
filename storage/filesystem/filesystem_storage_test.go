package filesystem

import (
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/kopia/repo/storage"

	"github.com/kopia/repo/internal/storagetesting"
)

func TestFileStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

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

		storagetesting.VerifyStorage(ctx, t, r)
		storagetesting.AssertConnectionInfoRoundTrips(ctx, t, r)
		if err := r.Close(ctx); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
}

func TestFileStorageTouch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t1 := "392ee1bc299db9f235e046a62625afb84902"
	t2 := "2a7ff4f29eddbcd4c18fa9e73fec20bbb71f"
	t3 := "0dae5918f83e6a24c8b3e274ca1026e43f24"

	path, _ := ioutil.TempDir("", "r-fs")
	defer os.RemoveAll(path)

	r, err := New(ctx, &Options{
		Path: path,
	})

	if r == nil || err != nil {
		t.Errorf("unexpected result: %v %v", r, err)
	}

	fs := r.(*fsStorage)
	fs.PutBlock(ctx, t1, []byte{1})
	time.Sleep(1 * time.Second) // sleep a bit to accommodate Apple filesystems with low timestamp resolution
	fs.PutBlock(ctx, t2, []byte{1})
	time.Sleep(1 * time.Second)
	fs.PutBlock(ctx, t3, []byte{1})

	verifyBlockTimestampOrder(t, fs, t1, t2, t3)

	fs.TouchBlock(ctx, t2, 1*time.Hour) // has no effect, all timestamps are very new
	verifyBlockTimestampOrder(t, fs, t1, t2, t3)

	fs.TouchBlock(ctx, t1, 0) // moves t1 to the top of the pile
	verifyBlockTimestampOrder(t, fs, t2, t3, t1)
	time.Sleep(1 * time.Second)

	fs.TouchBlock(ctx, t2, 0) // moves t2 to the top of the pile
	verifyBlockTimestampOrder(t, fs, t3, t1, t2)
	time.Sleep(1 * time.Second)

	fs.TouchBlock(ctx, t1, 0) // moves t1 to the top of the pile
	verifyBlockTimestampOrder(t, fs, t3, t2, t1)
}

func verifyBlockTimestampOrder(t *testing.T, st storage.Storage, want ...string) {
	blocks, err := storage.ListAllBlocks(context.Background(), st, "")
	if err != nil {
		t.Errorf("error listing blocks: %v", err)
		return
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Timestamp.Before(blocks[j].Timestamp)
	})

	var got []string
	for _, b := range blocks {
		got = append(got, b.BlockID)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("incorrect block order: %v, wanted %v", blocks, want)
	}
}
