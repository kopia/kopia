package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotEstimate(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	dir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file1.txt"), bytes.Repeat([]byte{1, 2, 3, 4, 5}, 15000), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file2.txt"), bytes.Repeat([]byte{2, 3, 4, 5, 6}, 10000), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "subdir", "file2.txt"), bytes.Repeat([]byte{3, 4, 5, 6, 7}, 5000), 0o600))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	out := env.RunAndExpectSuccess(t, "snapshot", "estimate", dir)
	require.Contains(t, out, "Snapshot includes 3 file(s), total size 150 KB")
	require.Contains(t, out, "Snapshot excludes no files.")
	require.Contains(t, out, "Snapshot excludes no directories.")

	// ignore some files
	env.RunAndExpectSuccess(t, "policy", "set", "--add-ignore", "*2.txt", dir)
	out = env.RunAndExpectSuccess(t, "snapshot", "estimate", dir)
	require.Contains(t, out, "Snapshot includes 1 file(s), total size 75 KB")
	require.Contains(t, out, "Snapshot excludes 2 file(s), total size 75 KB")
	require.Contains(t, out, " - file2.txt - 50 KB")
	require.Contains(t, out, " - subdir/file2.txt - 25 KB")
	require.Contains(t, out, "Snapshot excludes no directories.")

	// ignore a dir
	env.RunAndExpectSuccess(t, "policy", "set", "--add-ignore", "subdir", dir)
	out = env.RunAndExpectSuccess(t, "snapshot", "estimate", dir)
	require.Contains(t, out, "Snapshot includes 1 file(s), total size 75 KB")
	require.Contains(t, out, "Snapshot excludes 1 file(s), total size 50 KB")
	require.Contains(t, out, " - file2.txt - 50 KB")
	require.Contains(t, out, " - subdir")
	require.Contains(t, out, "Snapshot excludes 1 directories. Examples:")
}

func TestSnapshotEstimate_NotADirectory(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	dir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file1.txt"), []byte{1, 2, 3}, 0o600))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectFailure(t, "snapshot", "estimate", filepath.Join(dir, "file1.txt"))
}
