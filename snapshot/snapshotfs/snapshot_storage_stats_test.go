package snapshotfs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func TestCalculateStorageStats(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	sourceRoot := mockfs.NewDirectory()
	dir1 := sourceRoot.AddDir("dir1", 0o755)
	dir2 := sourceRoot.AddDir("dir2", 0o755)

	// root directory, 2 subdirectories + 2 unique files (dir1/file11 === dir2/file22)
	dir1.AddFile("file11", []byte{1, 2, 3}, 0o644)
	dir2.AddFile("file21", []byte{1, 2, 3, 4}, 0o644)
	dir2.AddFile("file22", []byte{1, 2, 3}, 0o644) // same content as dir11/file11

	src := snapshot.SourceInfo{
		Host:     env.Repository.ClientOptions().Hostname,
		UserName: env.Repository.ClientOptions().Username,
		Path:     "/dummy",
	}

	u := snapshotfs.NewUploader(env.RepositoryWriter)
	man1, err := u.Upload(ctx, sourceRoot, nil, src)
	require.NoError(t, err)
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	man2, err := u.Upload(ctx, sourceRoot, nil, src)
	require.NoError(t, err)
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	// add one more file, upload again
	dir2.AddFile("file23", []byte{1, 2, 3, 4, 5}, 0o644)

	man3, err := u.Upload(ctx, sourceRoot, nil, src)
	require.NoError(t, err)
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	var stats []*snapshot.Manifest

	require.NoError(t, snapshotfs.CalculateStorageStats(ctx, env.RepositoryWriter, []*snapshot.Manifest{
		man1, man2, man3,
	}, func(m *snapshot.Manifest) error {
		stats = append(stats, m)
		return nil
	}))

	require.Len(t, stats, 3)

	// first stat
	require.Equal(t, &snapshot.StorageStats{
		NewData: snapshot.StorageUsageDetails{
			FileObjectCount:      2,
			DirObjectCount:       3,
			ContentCount:         5,
			ObjectBytes:          7, // 3 + 4 unique bytes
			OriginalContentBytes: 7, // 3 + 4 unique bytes
			PackedContentBytes:   63,
		},
		RunningTotal: snapshot.StorageUsageDetails{
			FileObjectCount:      2,
			DirObjectCount:       3,
			ContentCount:         5,
			ObjectBytes:          7,
			OriginalContentBytes: 7, // 3 + 4 unique bytes
			PackedContentBytes:   63,
		},
	}, stats[0].StorageStats)

	// second has identical running totals and all-zero unique
	require.Equal(t, &snapshot.StorageStats{
		NewData: snapshot.StorageUsageDetails{}, // all-zero
		RunningTotal: snapshot.StorageUsageDetails{
			FileObjectCount:      2,
			DirObjectCount:       3,
			ContentCount:         5,
			ObjectBytes:          7,
			OriginalContentBytes: 7,
			PackedContentBytes:   63,
		},
	}, stats[1].StorageStats)

	// third has some additional file
	require.Equal(t, &snapshot.StorageStats{
		NewData: snapshot.StorageUsageDetails{
			FileObjectCount:      1,
			DirObjectCount:       2,
			ContentCount:         3,
			ObjectBytes:          5,
			OriginalContentBytes: 5,
			PackedContentBytes:   33,
		},
		RunningTotal: snapshot.StorageUsageDetails{
			FileObjectCount:      3,
			DirObjectCount:       5,
			ContentCount:         8,
			ObjectBytes:          12,
			OriginalContentBytes: 12,
			PackedContentBytes:   96,
		},
	}, stats[2].StorageStats)
}
