package filesystem

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kopia/kopia/repo/internal/storagetesting"
)

func TestFileStorage(t *testing.T) {
	ctx := context.Background()

	// Test varioush shard configurations.
	for _, shardSpec := range [][]int{
		[]int{0},
		[]int{1},
		[]int{3, 3},
		[]int{2},
		[]int{1, 1},
		[]int{1, 2},
		[]int{2, 2, 2},
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
	}
}
