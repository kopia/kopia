package sharded_test

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob/filesystem"
)

func TestShardedFileStorage(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	for _, parallel := range []int{0, 1, 4, 16} {
		t.Run(fmt.Sprintf("parallel-%v", parallel), func(t *testing.T) {
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

				r, err := filesystem.New(ctx, &filesystem.Options{
					Path:            path,
					DirectoryShards: shardSpec,
				})

				ioutil.WriteFile(filepath.Join(path, "foreign-file"), []byte{1, 2, 3}, 0600)

				if r == nil || err != nil {
					t.Errorf("unexpected result: %v %v", r, err)
				}

				blobtesting.VerifyStorage(ctx, t, r)
				blobtesting.AssertConnectionInfoRoundTrips(ctx, t, r)

				if err := r.Close(ctx); err != nil {
					t.Fatalf("err: %v", err)
				}
			}
		})
	}
}
