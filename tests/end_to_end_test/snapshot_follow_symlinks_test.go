package endtoend_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotCreateFollowSymlinks(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	tmp := testutil.TempDirectory(t)
	source := filepath.Join(tmp, "source")
	outside := filepath.Join(tmp, "outside")

	require.NoError(t, os.MkdirAll(filepath.Join(source, "real-dir"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(outside, "sub"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "real-file"), []byte("real file"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "real-dir", "x"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(outside, "o1"), []byte("outside file"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(outside, "sub", "o2"), []byte("outside nested file"), 0o600))

	symlinkOrSkip(t, filepath.Join(outside, "o1"), filepath.Join(source, "file-link"))
	symlinkOrSkip(t, outside, filepath.Join(source, "dir-link"))
	symlinkOrSkip(t, filepath.Join("real-dir", "x"), filepath.Join(source, "relative-link"))
	symlinkOrSkip(t, filepath.Join(tmp, "no-such-file"), filepath.Join(source, "dangling"))
	symlinkOrSkip(t, ".", filepath.Join(source, "loop"))

	snapshotAndRestore := func() string {
		t.Helper()

		e.RunAndExpectSuccess(t, "snapshot", "create", source)

		si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, source)
		require.Len(t, si, 1)
		require.NotEmpty(t, si[0].Snapshots)

		// setting times on a symlink to a directory fails on Windows.
		restored := testutil.TempDirectory(t)
		e.RunAndExpectSuccess(t, "snapshot", "restore", "--skip-times", si[0].Snapshots[len(si[0].Snapshots)-1].SnapshotID, restored)

		return restored
	}

	requireSymlink := func(path, wantTarget string) {
		t.Helper()

		fi, err := os.Lstat(path)
		require.NoError(t, err)
		require.NotZero(t, fi.Mode()&os.ModeSymlink, "%v should be a symlink", path)

		target, err := os.Readlink(path)
		require.NoError(t, err)
		require.Equal(t, wantTarget, target)
	}

	requireRegularFile := func(path, wantContent string) {
		t.Helper()

		fi, err := os.Lstat(path)
		require.NoError(t, err)
		require.True(t, fi.Mode().IsRegular(), "%v should be a regular file, is %v", path, fi.Mode())

		content, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, wantContent, string(content))
	}

	requireDirectory := func(path string) {
		t.Helper()

		fi, err := os.Lstat(path)
		require.NoError(t, err)
		require.True(t, fi.IsDir(), "%v should be a directory, is %v", path, fi.Mode())
	}

	restored := snapshotAndRestore()
	requireSymlink(filepath.Join(restored, "file-link"), filepath.Join(outside, "o1"))
	requireSymlink(filepath.Join(restored, "dir-link"), outside)
	requireSymlink(filepath.Join(restored, "relative-link"), filepath.Join("real-dir", "x"))
	requireRegularFile(filepath.Join(restored, "real-file"), "real file")

	e.RunAndExpectSuccess(t, "policy", "set", source, "--follow-symlinks=true")

	restored = snapshotAndRestore()

	requireRegularFile(filepath.Join(restored, "real-file"), "real file")
	requireRegularFile(filepath.Join(restored, "real-dir", "x"), "x")
	requireRegularFile(filepath.Join(restored, "file-link"), "outside file")
	requireRegularFile(filepath.Join(restored, "relative-link"), "x")
	requireDirectory(filepath.Join(restored, "dir-link"))
	requireRegularFile(filepath.Join(restored, "dir-link", "o1"), "outside file")
	requireRegularFile(filepath.Join(restored, "dir-link", "sub", "o2"), "outside nested file")

	requireSymlink(filepath.Join(restored, "dangling"), filepath.Join(tmp, "no-such-file"))
	requireSymlink(filepath.Join(restored, "loop"), ".")

	e.RunAndExpectSuccess(t, "policy", "set", source, "--follow-symlinks=false")

	restored = snapshotAndRestore()
	requireSymlink(filepath.Join(restored, "file-link"), filepath.Join(outside, "o1"))
	requireSymlink(filepath.Join(restored, "dir-link"), outside)
}

func TestSnapshotCreateFollowSymlinksWithNestedPolicy(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	tmp := testutil.TempDirectory(t)
	source := filepath.Join(tmp, "source")

	require.NoError(t, os.MkdirAll(filepath.Join(source, "sub"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "target"), []byte("target content"), 0o600))
	symlinkOrSkip(t, filepath.Join(tmp, "target"), filepath.Join(source, "top-link"))
	symlinkOrSkip(t, filepath.Join(tmp, "target"), filepath.Join(source, "sub", "nested-link"))

	e.RunAndExpectSuccess(t, "policy", "set", source, "--follow-symlinks=true")
	e.RunAndExpectSuccess(t, "policy", "set", filepath.Join(source, "sub"), "--max-file-size=1000000")
	e.RunAndExpectSuccess(t, "snapshot", "create", source)

	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, source)
	require.Len(t, si, 1)
	require.Len(t, si[0].Snapshots, 1)

	restored := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "snapshot", "restore", "--skip-times", si[0].Snapshots[0].SnapshotID, restored)

	for _, p := range []string{"top-link", filepath.Join("sub", "nested-link")} {
		content, err := os.ReadFile(filepath.Join(restored, p))
		require.NoError(t, err)
		require.Equal(t, "target content", string(content), p)

		fi, err := os.Lstat(filepath.Join(restored, p))
		require.NoError(t, err)
		require.True(t, fi.Mode().IsRegular(), "%v should be a regular file, is %v", p, fi.Mode())
	}
}

func symlinkOrSkip(t *testing.T, target, link string) {
	t.Helper()

	if err := os.Symlink(target, link); err != nil {
		t.Skipf("cannot create symbolic links: %v", err)
	}
}
