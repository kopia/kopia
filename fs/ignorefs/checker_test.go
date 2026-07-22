package ignorefs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestChecker_IsIgnored(t *testing.T) {
	t.Parallel()

	for _, tc := range cases {
		if tc.policyTree == oneFileSystemPolicy {
			continue
		}

		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			root := setupFilesystem(tc.skipDefaultFiles)
			if tc.setup != nil {
				tc.setup(root)
			}

			checker := ignorefs.NewChecker(root, tc.policyTree)
			ctx := testlogging.Context(t)

			allPaths := walkTree(t, root)
			visibleSet := toSet(walkTree(t, ignorefs.New(root, tc.policyTree)))

			for _, p := range allPaths {
				if p == "./" {
					continue
				}

				relPath := p[2:] // strip "./"
				isDir := p[len(p)-1] == '/'

				if isDir {
					relPath = relPath[:len(relPath)-1]
				}

				ignored, err := checker.IsIgnored(ctx, relPath, isDir)
				require.NoError(t, err)

				wantIgnored := !visibleSet[p]

				if p == "./largefile1" && wantIgnored && !ignored {
					continue
				}

				require.Equal(t, wantIgnored, ignored, "path %v", p)
			}
		})
	}
}

func TestChecker_IsIgnored_NestedDotIgnore(t *testing.T) {
	t.Parallel()

	root := setupFilesystem(true)
	root.AddFileLines(".kopiaignore", []string{"*.tmp"}, 0)
	root.AddFile("keep.txt", dummyFileContents, 0)
	root.AddFile("drop.tmp", dummyFileContents, 0)
	root.AddDir("A", 0)
	root.Subdir("A").AddFileLines(".kopiaignore", []string{"!special.tmp"}, 0)
	root.Subdir("A").AddFile("normal.tmp", dummyFileContents, 0)
	root.Subdir("A").AddFile("special.tmp", dummyFileContents, 0)

	checker := ignorefs.NewChecker(root, defaultPolicy)
	ctx := testlogging.Context(t)

	requireIgnored(t, checker, ctx, "drop.tmp", false, true)
	requireIgnored(t, checker, ctx, "keep.txt", false, false)
	requireIgnored(t, checker, ctx, "A/normal.tmp", false, true)
	requireIgnored(t, checker, ctx, "A/special.tmp", false, false)
}

func TestChecker_IsIgnored_MissingLiveDirectory(t *testing.T) {
	t.Parallel()

	root := setupFilesystem(true)
	root.AddFileLines(".kopiaignore", []string{"**/*.orphan"}, 0)
	root.AddFile("visible.txt", dummyFileContents, 0)

	checker := ignorefs.NewChecker(root, defaultPolicy)
	ctx := testlogging.Context(t)

	// Path exists only in snapshot, not on live disk - parent rules still apply.
	ignored, err := checker.IsIgnored(ctx, "gone/sub/file.orphan", false)
	require.NoError(t, err)
	require.True(t, ignored)

	ignored, err = checker.IsIgnored(ctx, "gone/sub/file.txt", false)
	require.NoError(t, err)
	require.False(t, ignored)
}

func requireIgnored(t *testing.T, checker *ignorefs.Checker, ctx context.Context, path string, isDir, want bool) {
	t.Helper()

	ignored, err := checker.IsIgnored(ctx, path, isDir)
	require.NoError(t, err)
	require.Equal(t, want, ignored, "path %v", path)
}

func toSet(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, it := range items {
		m[it] = true
	}

	return m
}
