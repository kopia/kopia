package ignorefs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs/ignorefs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/testutil"
)

func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()

	if err := os.Symlink(target, link); err != nil {
		t.Skipf("cannot create symbolic links: %v", err)
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()

	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(path, 0o700))
}

func Test_followSymlinks_localFilesystem(t *testing.T) {
	tmp := testutil.TempDirectory(t)
	src := filepath.Join(tmp, "src")
	outside := filepath.Join(tmp, "outside")

	mustMkdir(t, filepath.Join(src, "dir"))
	mustMkdir(t, filepath.Join(outside, "sub"))
	mustWriteFile(t, filepath.Join(src, "dir", "x"), "x-content")
	mustWriteFile(t, filepath.Join(outside, "o1"), "o1-content")
	mustWriteFile(t, filepath.Join(outside, "sub", "o2"), "o2-content")

	symlinkOrSkip(t, filepath.Join("dir", "x"), filepath.Join(src, "rel-file"))
	symlinkOrSkip(t, filepath.Join(outside, "o1"), filepath.Join(src, "abs-file"))
	symlinkOrSkip(t, outside, filepath.Join(src, "abs-dir"))
	symlinkOrSkip(t, filepath.Join("..", "..", "outside", "sub"), filepath.Join(src, "dir", "rel-dir"))
	symlinkOrSkip(t, filepath.Join(tmp, "missing"), filepath.Join(src, "dangling"))

	root, err := localfs.Directory(src)
	require.NoError(t, err)

	require.Equal(t, []string{
		"./abs-dir dir",
		"./abs-dir/o1 file:o1-content",
		"./abs-dir/sub dir",
		"./abs-dir/sub/o2 file:o2-content",
		"./abs-file file:o1-content",
		"./dangling symlink",
		"./dir dir",
		"./dir/rel-dir dir",
		"./dir/rel-dir/o2 file:o2-content",
		"./dir/x file:x-content",
		"./rel-file file:x-content",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(true))))

	root, err = localfs.Directory(src)
	require.NoError(t, err)

	require.Equal(t, []string{
		"./abs-dir symlink",
		"./abs-file symlink",
		"./dangling symlink",
		"./dir dir",
		"./dir/rel-dir symlink",
		"./dir/x file:x-content",
		"./rel-file symlink",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(false))))
}

func Test_followSymlinks_localFilesystemLoops(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	mustMkdir(t, filepath.Join(tmp, "a"))
	mustMkdir(t, filepath.Join(tmp, "b"))
	mustWriteFile(t, filepath.Join(tmp, "a", "afile"), "a")
	mustWriteFile(t, filepath.Join(tmp, "b", "bfile"), "b")

	symlinkOrSkip(t, ".", filepath.Join(tmp, "a", "self"))
	symlinkOrSkip(t, "..", filepath.Join(tmp, "a", "up"))
	symlinkOrSkip(t, filepath.Join("..", "b"), filepath.Join(tmp, "a", "toB"))
	symlinkOrSkip(t, filepath.Join("..", "a"), filepath.Join(tmp, "b", "toA"))

	root, err := localfs.Directory(tmp)
	require.NoError(t, err)

	require.Equal(t, []string{
		"./a dir",
		"./a/afile file:a",
		"./a/self symlink",
		"./a/toB dir",
		"./a/toB/bfile file:b",
		"./a/toB/toA symlink",
		"./a/up symlink",
		"./b dir",
		"./b/bfile file:b",
		"./b/toA dir",
		"./b/toA/afile file:a",
		"./b/toA/self symlink",
		"./b/toA/toB symlink",
		"./b/toA/up symlink",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(true))))
}

func Test_followSymlinks_localFilesystemDiamondIsNotALoop(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	mustMkdir(t, filepath.Join(tmp, "src"))
	mustMkdir(t, filepath.Join(tmp, "shared"))
	mustWriteFile(t, filepath.Join(tmp, "shared", "f"), "shared-content")

	symlinkOrSkip(t, filepath.Join("..", "shared"), filepath.Join(tmp, "src", "one"))
	symlinkOrSkip(t, filepath.Join("..", "shared"), filepath.Join(tmp, "src", "two"))

	root, err := localfs.Directory(filepath.Join(tmp, "src"))
	require.NoError(t, err)

	require.Equal(t, []string{
		"./one dir",
		"./one/f file:shared-content",
		"./two dir",
		"./two/f file:shared-content",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(true))))
}

func Test_followSymlinks_localFilesystemLoopThroughSymlinkedRootPath(t *testing.T) {
	tmp := testutil.TempDirectory(t)

	mustMkdir(t, filepath.Join(tmp, "real", "src"))
	mustWriteFile(t, filepath.Join(tmp, "real", "src", "file"), "content")
	symlinkOrSkip(t, "real", filepath.Join(tmp, "alias"))
	symlinkOrSkip(t, "..", filepath.Join(tmp, "real", "src", "up"))

	root, err := localfs.Directory(filepath.Join(tmp, "alias", "src"))
	require.NoError(t, err)

	require.Equal(t, []string{
		"./file file:content",
		"./up symlink",
	}, describeTree(t, ignorefs.New(root, followSymlinksPolicy(true))))
}
