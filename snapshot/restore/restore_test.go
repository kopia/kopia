package restore

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/mockfs"
)

func TestValidateEntryName(t *testing.T) {
	for _, name := range []string{"file.txt", "dir", "..hidden", "a b", "name.with.dots"} {
		require.NoError(t, validateEntryName(name), name)
	}

	for _, name := range []string{"", ".", "..", "sub/file", "../escaped", "a/../b", "/abs", string(os.PathSeparator) + "x"} {
		require.Error(t, validateEntryName(name), name)
	}
}

func TestEntryRejectsNamesEscapingTargetDirectory(t *testing.T) {
	for _, name := range []string{"..", "."} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			baseDir := t.TempDir()
			targetDir := filepath.Join(baseDir, "target")

			root := mockfs.NewDirectory()
			root.AddDir(name, 0o755).AddFile("escaped.txt", []byte("escaped"), 0o644)

			out := &FilesystemOutput{
				TargetPath:           targetDir,
				OverwriteDirectories: true,
				OverwriteFiles:       true,
				SkipOwners:           true,
				SkipPermissions:      true,
				SkipTimes:            true,
			}
			require.NoError(t, out.Init(ctx))

			_, err := Entry(ctx, nil, out, root, Options{Parallel: 1, RestoreDirEntryAtDepth: math.MaxInt32})
			require.ErrorContains(t, err, "invalid entry name")

			// nothing may be written outside of (or into) the restore target.
			require.NoFileExists(t, filepath.Join(baseDir, "escaped.txt"))
			require.NoFileExists(t, filepath.Join(targetDir, "escaped.txt"))
		})
	}
}

func TestEntryRestoresRegularNames(t *testing.T) {
	ctx := context.Background()
	targetDir := filepath.Join(t.TempDir(), "target")

	root := mockfs.NewDirectory()
	root.AddDir("sub", 0o755).AddFile("file.txt", []byte("content"), 0o644)

	out := &FilesystemOutput{
		TargetPath:           targetDir,
		OverwriteDirectories: true,
		OverwriteFiles:       true,
		SkipOwners:           true,
		SkipPermissions:      true,
		SkipTimes:            true,
	}
	require.NoError(t, out.Init(ctx))

	_, err := Entry(ctx, nil, out, root, Options{Parallel: 1, RestoreDirEntryAtDepth: math.MaxInt32})
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(targetDir, "sub", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, "content", string(data))
}
