package restore

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/testlogging"
)

func TestOverwriteReadOnlyFile(t *testing.T) {
	t.Parallel()

	for _, atomic := range []bool{false, true} {
		t.Run("atomic="+strconv.FormatBool(atomic), func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			sourcePath := filepath.Join(dir, "source")
			targetDir := filepath.Join(dir, "target")
			targetPath := filepath.Join(targetDir, "restored")

			require.NoError(t, os.WriteFile(sourcePath, []byte("snapshot contents"), 0o600))
			require.NoError(t, os.Chmod(sourcePath, 0o400))
			require.NoError(t, os.Mkdir(targetDir, 0o700))

			t.Cleanup(func() {
				_ = os.Chmod(sourcePath, 0o600)
				_ = os.Chmod(targetPath, 0o600)
			})

			entry, err := localfs.NewEntry(sourcePath)
			require.NoError(t, err)

			file, ok := entry.(fs.File)
			require.True(t, ok)

			output := &FilesystemOutput{
				TargetPath:           targetDir,
				OverwriteFiles:       true,
				WriteFilesAtomically: atomic,
			}
			require.NoError(t, output.Init(testlogging.Context(t)))

			require.NoError(t, output.WriteFile(testlogging.Context(t), "restored", file, nil))
			require.NoError(t, output.WriteFile(testlogging.Context(t), "restored", file, nil))

			contents, err := os.ReadFile(targetPath)
			require.NoError(t, err)
			require.Equal(t, "snapshot contents", string(contents))

			info, err := os.Stat(targetPath)
			require.NoError(t, err)
			require.Zero(t, info.Mode().Perm()&0o200)
		})
	}
}
