package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotStorageStats(t *testing.T) {
	env := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, testenv.NewInProcRunner(t))

	dir1 := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte{1, 2, 3, 4, 5}, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir1, "file2.txt"), []byte{2, 3, 4, 5, 6}, 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir1, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir1, "subdir", "file2.txt"), []byte{3, 4, 5, 6, 7}, 0o600))

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)
	env.RunAndExpectSuccess(t, "snapshot", "create", dir1)

	// same as ./file1.txt
	require.NoError(t, os.WriteFile(filepath.Join(dir1, "subdir", "file3.txt"), []byte{1, 2, 3, 4, 5}, 0o600))

	// new file
	require.NoError(t, os.WriteFile(filepath.Join(dir1, "subdir", "file4.txt"), []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0o600))
	env.RunAndExpectSuccess(t, "snapshot", "create", dir1)

	var manifests []cli.SnapshotManifest

	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "ls", "--storage-stats", dir1, "--json"), &manifests)
	require.Len(t, manifests, 2)

	require.Equal(t, &snapshot.StorageStats{
		NewData: snapshot.StorageUsageDetails{
			FileObjectCount:      3,
			DirObjectCount:       2,
			ContentCount:         5,
			ObjectBytes:          15,
			OriginalContentBytes: 15,
			PackedContentBytes:   99,
		},
		RunningTotal: snapshot.StorageUsageDetails{
			FileObjectCount:      3,
			DirObjectCount:       2,
			ContentCount:         5,
			ObjectBytes:          15,
			OriginalContentBytes: 15,
			PackedContentBytes:   99,
		},
	}, manifests[0].StorageStats)

	require.Equal(t, &snapshot.StorageStats{
		NewData: snapshot.StorageUsageDetails{
			FileObjectCount:      1,
			DirObjectCount:       2,
			ContentCount:         3,
			ObjectBytes:          8,
			OriginalContentBytes: 8,
			PackedContentBytes:   36,
		},
		RunningTotal: snapshot.StorageUsageDetails{
			FileObjectCount:      4,
			DirObjectCount:       4,
			ContentCount:         8,
			ObjectBytes:          23,
			OriginalContentBytes: 23,
			PackedContentBytes:   135,
		},
	}, manifests[1].StorageStats)

	// same but in reverse order
	testutil.MustParseJSONLines(t, env.RunAndExpectSuccess(t, "snapshot", "ls", "--storage-stats", dir1, "--reverse", "--json"), &manifests)
	require.Len(t, manifests, 2)

	require.Equal(t, &snapshot.StorageStats{
		NewData: snapshot.StorageUsageDetails{
			ObjectBytes:          23,
			OriginalContentBytes: 23,
			PackedContentBytes:   135,
			FileObjectCount:      4,
			DirObjectCount:       2,
			ContentCount:         6,
		},
		RunningTotal: snapshot.StorageUsageDetails{
			ObjectBytes:          23,
			OriginalContentBytes: 23,
			PackedContentBytes:   135,
			FileObjectCount:      4,
			DirObjectCount:       2,
			ContentCount:         6,
		},
	}, manifests[0].StorageStats)

	require.Equal(t, &snapshot.StorageStats{
		NewData: snapshot.StorageUsageDetails{
			ObjectBytes:          0, // all file data was already present
			OriginalContentBytes: 0,
			PackedContentBytes:   0,
			FileObjectCount:      0,
			DirObjectCount:       2, // new directories only
			ContentCount:         2,
		},
		RunningTotal: snapshot.StorageUsageDetails{
			ObjectBytes:          23,
			OriginalContentBytes: 23,
			PackedContentBytes:   135,
			FileObjectCount:      4,
			DirObjectCount:       4,
			ContentCount:         8,
		},
	}, manifests[1].StorageStats)

	out := env.RunAndExpectSuccess(t, "snapshot", "ls", "--storage-stats")
	require.Len(t, out, 3)
	require.Contains(t, out[1], "new-data:99 B ")
	require.Contains(t, out[1], "new-files:3 ")
	require.Contains(t, out[1], "new-dirs:2 ")
	require.Contains(t, out[2], "new-data:36 B ")
	require.Contains(t, out[2], "new-files:1 ")
	require.Contains(t, out[2], "new-dirs:2 ")

	out = env.RunAndExpectSuccess(t, "snapshot", "ls", "--storage-stats", "--reverse")
	require.Len(t, out, 3)
	require.Contains(t, out[1], "new-data:135 B ")
	require.Contains(t, out[1], "new-files:4 ")
	require.Contains(t, out[1], "new-dirs:2 ")
	require.Contains(t, out[2], "new-data:0 B ")
	require.Contains(t, out[2], "new-files:0 ")
	require.Contains(t, out[2], "new-dirs:2 ")
}
