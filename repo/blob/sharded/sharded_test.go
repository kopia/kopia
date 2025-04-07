package sharded_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/blob/sharded"
)

func TestShardedOpenLegacyFileStorage(t *testing.T) {
	t.Parallel()
	ctx := testlogging.Context(t)
	dir := testutil.TempDirectory(t)

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path:    dir,
		Options: sharded.Options{},
	}, false)

	require.NoError(t, err)
	require.NoError(t, st.PutBlob(ctx, "foobarbaz12345678910123213123", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	require.FileExists(t, filepath.Join(dir, "foo", "bar", "baz12345678910123213123.f"))
}

func TestShardedOpenLatestFileStorage(t *testing.T) {
	t.Parallel()
	ctx := testlogging.Context(t)
	dir := testutil.TempDirectory(t)

	st, err := filesystem.New(ctx, &filesystem.Options{
		Path:    dir,
		Options: sharded.Options{},
	}, true)

	require.NoError(t, err)
	require.NoError(t, st.PutBlob(ctx, "foobarbaz12345678910123213123", gather.FromSlice([]byte{1, 2, 3}), blob.PutOptions{}))
	require.FileExists(t, filepath.Join(dir, "f", "oob", "arbaz12345678910123213123.f"))
}

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
					Path: path,
					Options: sharded.Options{
						DirectoryShards: shardSpec,
					},
				}, true)

				os.WriteFile(filepath.Join(path, "foreign-file"), []byte{1, 2, 3}, 0o600)

				if r == nil || err != nil {
					t.Errorf("unexpected result: %v %v", r, err)
				}

				blobtesting.VerifyStorage(ctx, t, r, blob.PutOptions{})
				blobtesting.AssertConnectionInfoRoundTrips(ctx, t, r)

				if err := r.Close(ctx); err != nil {
					t.Fatalf("err: %v", err)
				}
			}
		})
	}
}

func TestShardedFileStorageShardingMap(t *testing.T) {
	cases := []struct {
		desc            string
		shardMapJSON    string
		blobFilePathMap map[blob.ID]string
	}{
		{
			"case1",
			`
			{
				"default": [3,2,1],
				"overrides": [
					{ "prefix": "p", "shards": [2,2] },
					{ "prefix": "x", "shards": [1,1,1] }
				],
				"maxNonShardedLength": 2
			}`,
			map[blob.ID]string{
				// non-sharded because of ID length
				"ab": "ab.f",

				// sharded according to default shards - 3,2,1
				"defaultsharded": "def/au/l/tsharded.f",

				// sharded according to first override
				"phello":   "ph/el/lo.f",
				"pgoodbye": "pg/oo/dbye.f",

				// second override
				"xhello": "x/h/e/llo.f",
				"xbye":   "x/b/y/e.f",
				"xo":     "xo.f",
			},
		},
		{
			"shorter-than-nonsharded-length",
			`
			{
				"default": [3,3],
				"maxNonShardedLength": 10
			}`,
			map[blob.ID]string{
				"foobarbar":    "foobarbar.f",
				"foobarbarbar": "foo/bar/barbar.f",
			},
		},
		{
			"shorter-than-shards",
			`
			{
				"default": [3,3],
				"maxNonShardedLength": 0
			}`,
			map[blob.ID]string{
				"fo":      "fo.f",
				"foo":     "foo.f",
				"foob":    "foo/b.f",
				"fooba":   "foo/ba.f",
				"foobar":  "foo/bar.f",
				"foobar2": "foo/bar/2.f",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testlogging.Context(t)

			path := testutil.TempDirectory(t)

			r, err := filesystem.New(ctx, &filesystem.Options{
				Path: path,
			}, true)
			require.NoError(t, err)

			dotShardsFile := filepath.Join(path, ".shards")

			// write shards file
			require.NoError(t, os.WriteFile(dotShardsFile, []byte(tc.shardMapJSON), 0o600))

			var allBlobIDs []blob.ID

			for blobID, wantFilename := range tc.blobFilePathMap {
				require.NoError(t, r.PutBlob(ctx, blobID, gather.FromSlice([]byte("foo")), blob.PutOptions{}))
				require.FileExists(t, filepath.Join(path, wantFilename))

				allBlobIDs = append(allBlobIDs, blobID)
			}

			for _, blobID := range allBlobIDs {
				for i := range len(blobID) {
					prefix := blobID[0:i]

					var wantMatches []blob.ID

					for _, b2 := range allBlobIDs {
						if strings.HasPrefix(string(b2), string(prefix)) {
							wantMatches = append(wantMatches, b2)
						}
					}

					blobtesting.AssertListResultsIDs(ctx, t, r, prefix, wantMatches...)
				}
			}
		})
	}
}

func TestShardedFileStorageShardingMap_Invalid(t *testing.T) {
	t.Parallel()

	ctx := testlogging.Context(t)

	path := testutil.TempDirectory(t)

	r, err := filesystem.New(ctx, &filesystem.Options{
		Path: path,
	}, true)
	require.NoError(t, err)

	dotShardsFile := filepath.Join(path, ".shards")

	// write malformed shards file
	require.NoError(t, os.WriteFile(dotShardsFile, []byte{1, 2, 3}, 0o600))

	var tmp gather.WriteBuffer
	defer tmp.Close()

	require.Error(t, r.PutBlob(ctx, "someblob", gather.FromSlice([]byte("foo")), blob.PutOptions{}))

	// delete invalid .shards file
	require.NoError(t, os.Remove(dotShardsFile))

	// now putting the blob will succeed
	require.NoError(t, r.PutBlob(ctx, "someblob", gather.FromSlice([]byte("foo")), blob.PutOptions{}))

	// write malformed file again, but will be ignored since it was successfully loaded
	// in this session.
	require.NoError(t, os.WriteFile(dotShardsFile, []byte{1, 2, 3}, 0o600))

	require.NoError(t, r.PutBlob(ctx, "someblob2", gather.FromSlice([]byte("foo")), blob.PutOptions{}))
}

func TestClone(t *testing.T) {
	p := &sharded.Parameters{
		DefaultShards:   []int{1, 2, 3},
		UnshardedLength: 4,
		Overrides: []sharded.PrefixAndShards{
			{"x", []int{3, 2, 1}},
			{"y", []int{4, 3, 2}},
		},
	}

	var buf1 bytes.Buffer

	require.NoError(t, p.Save(&buf1))

	var buf2 bytes.Buffer

	p2 := p.Clone()
	require.NoError(t, p2.Save(&buf2))

	require.Equal(t, buf1.String(), buf2.String())

	// change 'p' in place, p2 should not change
	p.DefaultShards[0]++
	p.UnshardedLength++
	p.Overrides[0].Prefix = "zz"
	p.Overrides[0].Shards[2]++

	var buf2after bytes.Buffer

	require.NoError(t, p2.Save(&buf2after))

	require.Equal(t, buf2.String(), buf2after.String())
}
