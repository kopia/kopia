package ignorefs

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/snapshot/policy"
)

func Test_isSameOrAncestorPath(t *testing.T) {
	cases := []struct {
		name     string
		ancestor string
		path     string
		sep      string
		foldCase bool
		want     bool
	}{
		{"same", "/a/b", "/a/b", "/", false, true},
		{"same with trailing separator", "/a/b/", "/a/b", "/", false, true},
		{"child", "/a/b", "/a/b/c/d", "/", false, true},
		{"unrelated sibling", "/a/b", "/a/c", "/", false, false},
		{"shared prefix is not an ancestor", "/a/b", "/a/bc", "/", false, false},
		{"descendant is not an ancestor", "/a/b/c", "/a/b", "/", false, false},
		{"root contains everything", "/", "/a/b", "/", false, true},
		{"case matters on case-sensitive systems", "/a/B", "/a/b/c", "/", false, false},
		{"windows same ignoring case", `C:\Data\Src`, `c:\data\src`, `\`, true, true},
		{"windows child ignoring case", `C:\Data`, `c:\DATA\src\x`, `\`, true, true},
		{"windows drive root", `C:\`, `C:\data`, `\`, true, true},
		{"windows other drive", `C:\`, `D:\data`, `\`, true, false},
		{"windows shared prefix", `C:\Data`, `C:\Database`, `\`, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isSameOrAncestorPath(tc.ancestor, tc.path, tc.sep, tc.foldCase))
		})
	}
}

func Test_followSymlinks_depthLimitStopsDirectoryLoop(t *testing.T) {
	// mockfs has no local paths, so only the depth limit can stop the loop.
	root := mockfs.NewDirectory()
	root.AddDir("d", 0o755).AddSymlink("self", "../d", 0o777)

	followed := policy.OptionalBool(true)
	tree := policy.BuildTree(map[string]*policy.Policy{
		".": {FilesPolicy: policy.FilesPolicy{FollowSymlinks: &followed}},
	}, policy.DefaultPolicy)

	var (
		deepest int
		walk    func(path string, d fs.Directory)
	)

	walk = func(path string, d fs.Directory) {
		deepest = max(deepest, strings.Count(path, "/self"))

		require.NoError(t, fs.IterateEntries(testlogging.Context(t), d, func(_ context.Context, e fs.Entry) error {
			if sub, ok := e.(fs.Directory); ok {
				walk(path+"/"+e.Name(), sub)
			}

			return nil
		}))
	}

	walk(".", New(root, tree))

	require.Equal(t, maxFollowedDirDepth, deepest)
}
