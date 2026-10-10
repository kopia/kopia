//go:build !windows

package restore_test

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/snapshot/upload"
)

// hardlinkSource creates a source tree:
//
//	a            (content X, 3 links: a, sub/b, sub/deep/c)
//	sub/b
//	sub/deep/c
//	twin         (content X, no links - must NOT be linked to a)
//	pair1, pair2 (content Y, 2 links)
//	solo         (content Z)
func hardlinkSource(t *testing.T) string {
	t.Helper()

	src := testutil.TempDirectory(t)

	require.NoError(t, os.MkdirAll(filepath.Join(src, "sub", "deep"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "a"), []byte("content X"), 0o644))
	require.NoError(t, os.Link(filepath.Join(src, "a"), filepath.Join(src, "sub", "b")))
	require.NoError(t, os.Link(filepath.Join(src, "a"), filepath.Join(src, "sub", "deep", "c")))
	require.NoError(t, os.WriteFile(filepath.Join(src, "twin"), []byte("content X"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "pair1"), []byte("content Y"), 0o600))
	require.NoError(t, os.Link(filepath.Join(src, "pair1"), filepath.Join(src, "pair2")))
	require.NoError(t, os.WriteFile(filepath.Join(src, "solo"), []byte("content Z"), 0o644))

	return src
}

func snapshotWithHardlinks(ctx context.Context, t *testing.T, env *repotesting.Environment, src string, track bool) fs.Entry {
	t.Helper()

	root, _ := snapshotWithHardlinksAndStats(ctx, t, env, src, track, false)

	return root
}

func snapshotWithHardlinksAndStats(ctx context.Context, t *testing.T, env *repotesting.Environment, src string, track, reuse bool) (fs.Entry, snapshot.Stats) {
	t.Helper()

	srcDir, err := localfs.Directory(src)
	require.NoError(t, err)

	pol := *policy.DefaultPolicy
	pol.FilesPolicy.TrackHardlinks = policy.NewOptionalBool(policy.OptionalBool(track))
	pol.UploadPolicy.ReuseHardlinkContent = policy.NewOptionalBool(policy.OptionalBool(reuse))

	u := upload.NewUploader(env.RepositoryWriter)
	man, err := u.Upload(ctx, srcDir, policy.BuildTree(nil, &pol), snapshot.SourceInfo{})
	require.NoError(t, err)

	root, err := snapshotfs.SnapshotRoot(env.RepositoryWriter, man)
	require.NoError(t, err)

	return root, man.Stats
}

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()

	st, err := os.Lstat(path)
	require.NoError(t, err)

	return testutil.EnsureType[*syscall.Stat_t](t, st.Sys()).Ino
}

func nlinkOf(t *testing.T, path string) uint64 {
	t.Helper()

	st, err := os.Lstat(path)
	require.NoError(t, err)

	return uint64(testutil.EnsureType[*syscall.Stat_t](t, st.Sys()).Nlink) //nolint:unconvert
}

func restoreToDir(ctx context.Context, t *testing.T, env *repotesting.Environment, root fs.Entry, target string, opts restore.Options, fso *restore.FilesystemOutput) restore.Stats {
	t.Helper()

	fso.TargetPath = target
	fso.OverwriteFiles = true
	fso.OverwriteDirectories = true
	fso.SkipOwners = true

	require.NoError(t, fso.Init(ctx))

	st, err := restore.Entry(ctx, env.RepositoryWriter, fso, root, opts)
	require.NoError(t, err)

	return st
}

func TestRestoreHardlinks(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	src := hardlinkSource(t)
	root := snapshotWithHardlinks(ctx, t, env, src, true)

	for _, parallel := range []int{1, 8} {
		target := testutil.TempDirectory(t)

		restoreToDir(ctx, t, env, root, target, restore.Options{Parallel: parallel, RestoreDirEntryAtDepth: math.MaxInt32}, &restore.FilesystemOutput{})

		for _, name := range []string{"a", "sub/b", "sub/deep/c", "twin", "pair1", "pair2", "solo"} {
			want, err := os.ReadFile(filepath.Join(src, name))
			require.NoError(t, err)

			got, err := os.ReadFile(filepath.Join(target, name))
			require.NoError(t, err)

			require.Equal(t, want, got, name)
		}

		a := inodeOf(t, filepath.Join(target, "a"))
		require.Equal(t, a, inodeOf(t, filepath.Join(target, "sub", "b")), "parallel=%d", parallel)
		require.Equal(t, a, inodeOf(t, filepath.Join(target, "sub", "deep", "c")), "parallel=%d", parallel)
		require.Equal(t, uint64(3), nlinkOf(t, filepath.Join(target, "a")), "parallel=%d", parallel)

		require.NotEqual(t, a, inodeOf(t, filepath.Join(target, "twin")), "identical content must not be linked")
		require.Equal(t, uint64(1), nlinkOf(t, filepath.Join(target, "twin")))

		require.Equal(t, inodeOf(t, filepath.Join(target, "pair1")), inodeOf(t, filepath.Join(target, "pair2")))
		require.Equal(t, uint64(2), nlinkOf(t, filepath.Join(target, "pair1")))
		require.Equal(t, uint64(1), nlinkOf(t, filepath.Join(target, "solo")))

		// attributes apply to the shared inode.
		st, err := os.Stat(filepath.Join(target, "pair2"))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), st.Mode().Perm())
	}
}

// TestRestoreHardlinksReusedContent covers the full round trip on a real
// filesystem: reuse content of hardlinks at upload time, then recreate the
// links at restore time.
func TestRestoreHardlinksReusedContent(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	src := hardlinkSource(t)
	root, stats := snapshotWithHardlinksAndStats(ctx, t, env, src, true, true)

	// a, sub/b, sub/deep/c share one inode; pair1/pair2 share another.
	require.Equal(t, int32(3), stats.CachedFiles, "two 3-link + one 2-link members must be reused")
	require.Equal(t, int32(4), stats.NonCachedFiles)

	target := testutil.TempDirectory(t)
	restoreToDir(ctx, t, env, root, target, restore.Options{Parallel: 8, RestoreDirEntryAtDepth: math.MaxInt32}, &restore.FilesystemOutput{})

	for _, name := range []string{"a", "sub/b", "sub/deep/c", "twin", "pair1", "pair2", "solo"} {
		want, err := os.ReadFile(filepath.Join(src, name))
		require.NoError(t, err)

		got, err := os.ReadFile(filepath.Join(target, name))
		require.NoError(t, err)
		require.Equal(t, want, got, name)
	}

	require.Equal(t, uint64(3), nlinkOf(t, filepath.Join(target, "a")))
	require.Equal(t, uint64(2), nlinkOf(t, filepath.Join(target, "pair1")))
	require.Equal(t, uint64(1), nlinkOf(t, filepath.Join(target, "twin")))
}

func TestRestoreHardlinksSkipped(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	src := hardlinkSource(t)
	root := snapshotWithHardlinks(ctx, t, env, src, true)
	target := testutil.TempDirectory(t)

	restoreToDir(ctx, t, env, root, target, restore.Options{RestoreDirEntryAtDepth: math.MaxInt32}, &restore.FilesystemOutput{SkipHardlinks: true})

	require.NotEqual(t, inodeOf(t, filepath.Join(target, "a")), inodeOf(t, filepath.Join(target, "sub", "b")))
	require.Equal(t, uint64(1), nlinkOf(t, filepath.Join(target, "a")))
	require.Equal(t, uint64(1), nlinkOf(t, filepath.Join(target, "pair1")))
}

func TestRestoreHardlinksNotTracked(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	src := hardlinkSource(t)
	root := snapshotWithHardlinks(ctx, t, env, src, false)
	target := testutil.TempDirectory(t)

	restoreToDir(ctx, t, env, root, target, restore.Options{RestoreDirEntryAtDepth: math.MaxInt32}, &restore.FilesystemOutput{})

	// policy was off at snapshot time, so nothing to link.
	require.NotEqual(t, inodeOf(t, filepath.Join(target, "a")), inodeOf(t, filepath.Join(target, "sub", "b")))
	require.Equal(t, uint64(1), nlinkOf(t, filepath.Join(target, "a")))
}

func TestRestoreHardlinksIncremental(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	src := hardlinkSource(t)
	root := snapshotWithHardlinks(ctx, t, env, src, true)
	target := testutil.TempDirectory(t)

	// pre-populate the target with one member of the group so that it is skipped.
	srcInfo, err := os.Stat(filepath.Join(src, "a"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(target, "a"), []byte("content X"), 0o644))
	require.NoError(t, os.Chtimes(filepath.Join(target, "a"), srcInfo.ModTime(), srcInfo.ModTime()))

	st := restoreToDir(ctx, t, env, root, target, restore.Options{Incremental: true, RestoreDirEntryAtDepth: math.MaxInt32}, &restore.FilesystemOutput{})
	require.Equal(t, int32(1), st.SkippedCount)

	a := inodeOf(t, filepath.Join(target, "a"))
	require.Equal(t, a, inodeOf(t, filepath.Join(target, "sub", "b")), "skipped existing file must anchor its link group")
	require.Equal(t, a, inodeOf(t, filepath.Join(target, "sub", "deep", "c")))
	require.Equal(t, uint64(3), nlinkOf(t, filepath.Join(target, "a")))
}

func TestRestoreHardlinksTar(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	src := hardlinkSource(t)
	root := snapshotWithHardlinks(ctx, t, env, src, true)

	var buf bytes.Buffer

	_, err := restore.Entry(ctx, env.RepositoryWriter, restore.NewTarOutput(nopWriteCloser{&buf}), root, restore.Options{RestoreDirEntryAtDepth: math.MaxInt32})
	require.NoError(t, err)

	type tarEntry struct {
		typeflag byte
		linkname string
		size     int64
	}

	entries := map[string]tarEntry{}
	tr := tar.NewReader(&buf)

	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		entries[h.Name] = tarEntry{h.Typeflag, h.Linkname, h.Size}
	}

	// tar output is sequential and directory entries are sorted, so the first
	// member of each group in traversal order is the regular file.
	regular := 0
	links := 0

	for _, name := range []string{"a", "sub/b", "sub/deep/c"} {
		e := entries[name]

		switch e.typeflag {
		case tar.TypeReg:
			regular++

			require.Equal(t, int64(len("content X")), e.size)
		case tar.TypeLink:
			links++

			target := entries[e.linkname]
			require.Equal(t, byte(tar.TypeReg), target.typeflag, "link target %q must be a regular file", e.linkname)
			require.Equal(t, int64(0), e.size)
		default:
			t.Fatalf("unexpected tar type %v for %q", e.typeflag, name)
		}
	}

	require.Equal(t, 1, regular)
	require.Equal(t, 2, links)

	require.Equal(t, byte(tar.TypeReg), entries["twin"].typeflag)
	require.Equal(t, byte(tar.TypeReg), entries["solo"].typeflag)

	pairTypes := []byte{entries["pair1"].typeflag, entries["pair2"].typeflag}
	require.ElementsMatch(t, []byte{tar.TypeReg, tar.TypeLink}, pairTypes)
}

// TestRestoreHardlinksManyParallel restores many link groups spread across
// directories with high parallelism, so that members of a group are regularly
// processed on different workers in arbitrary order.
func TestRestoreHardlinksManyParallel(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	const (
		groups       = 50
		linksPerFile = 4
		dirs         = 5
	)

	src := testutil.TempDirectory(t)

	for d := range dirs {
		require.NoError(t, os.Mkdir(filepath.Join(src, "d"+itoa(d)), 0o755))
	}

	for g := range groups {
		first := filepath.Join(src, "d0", "g"+itoa(g))
		require.NoError(t, os.WriteFile(first, []byte("group "+itoa(g)), 0o644))

		for l := 1; l < linksPerFile; l++ {
			require.NoError(t, os.Link(first, filepath.Join(src, "d"+itoa(l%dirs), "g"+itoa(g)+"-"+itoa(l))))
		}
	}

	root := snapshotWithHardlinks(ctx, t, env, src, true)
	target := testutil.TempDirectory(t)

	restoreToDir(ctx, t, env, root, target, restore.Options{Parallel: 16, RestoreDirEntryAtDepth: math.MaxInt32}, &restore.FilesystemOutput{})

	for g := range groups {
		first := filepath.Join(target, "d0", "g"+itoa(g))
		ino := inodeOf(t, first)

		require.Equal(t, uint64(linksPerFile), nlinkOf(t, first), "group %d", g)

		for l := 1; l < linksPerFile; l++ {
			p := filepath.Join(target, "d"+itoa(l%dirs), "g"+itoa(g)+"-"+itoa(l))
			require.Equal(t, ino, inodeOf(t, p), p)

			got, err := os.ReadFile(p)
			require.NoError(t, err)
			require.Equal(t, "group "+itoa(g), string(got))
		}
	}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }
