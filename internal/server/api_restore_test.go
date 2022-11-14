package server_test

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/internal/uitask"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func TestRestoreSnapshots(t *testing.T) {
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	si1 := env.LocalPathSourceInfo("/dummy/path")

	var id11 manifest.ID

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{Purpose: "Test"}, func(ctx context.Context, w repo.RepositoryWriter) error {
		u := snapshotfs.NewUploader(w)

		dir1 := mockfs.NewDirectory()

		dir1.AddFile("file1", []byte{1, 2, 3}, 0o644)
		dir1.AddDir("dir1", 0o644).AddFile("file2", []byte{1, 2, 4}, 0o644)

		man11, err := u.Upload(ctx, dir1, nil, si1)
		require.NoError(t, err)
		id11, err = snapshot.SaveSnapshot(ctx, w, man11)
		require.NoError(t, err)

		return nil
	}))

	srvInfo := startServer(t, env, false)

	cli, err := apiclient.NewKopiaAPIClient(apiclient.Options{
		BaseURL:                             srvInfo.BaseURL,
		TrustedServerCertificateFingerprint: srvInfo.TrustedServerCertificateFingerprint,
		Username:                            testUIUsername,
		Password:                            testUIPassword,
	})

	require.NoError(t, err)
	require.NoError(t, cli.FetchCSRFTokenForTesting(ctx))

	t.Run("Filesystem", func(t *testing.T) {
		targetPath1 := testutil.TempDirectory(t)
		restoreTask1, err := serverapi.Restore(ctx, cli, &serverapi.RestoreRequest{
			Root: string(id11),
			Options: restore.Options{
				RestoreDirEntryAtDepth: math.MaxInt32,
			},
			Filesystem: &restore.FilesystemOutput{
				TargetPath:      targetPath1,
				SkipOwners:      true,
				SkipPermissions: true,
			},
		})

		require.NoError(t, err)
		waitForTask(t, cli, restoreTask1.TaskID, 30*time.Second)
		require.FileExists(t, filepath.Join(targetPath1, "file1"))
		require.FileExists(t, filepath.Join(targetPath1, "dir1", "file2"))
	})

	t.Run("FilesystemSubdir", func(t *testing.T) {
		targetPath1 := testutil.TempDirectory(t)
		restoreTask1, err := serverapi.Restore(ctx, cli, &serverapi.RestoreRequest{
			Root: string(id11) + "/dir1",
			Options: restore.Options{
				RestoreDirEntryAtDepth: math.MaxInt32,
			},
			Filesystem: &restore.FilesystemOutput{
				TargetPath:      targetPath1,
				SkipOwners:      true,
				SkipPermissions: true,
			},
		})

		require.NoError(t, err)
		require.Equal(t, uitask.StatusSuccess, waitForTask(t, cli, restoreTask1.TaskID, 30*time.Second).Status)

		require.FileExists(t, filepath.Join(targetPath1, "file2"))
	})

	t.Run("FilesystemFullShallowRestore", func(t *testing.T) {
		targetPath1 := testutil.TempDirectory(t)
		restoreTask1, err := serverapi.Restore(ctx, cli, &serverapi.RestoreRequest{
			Root:    string(id11),
			Options: restore.Options{},
			Filesystem: &restore.FilesystemOutput{
				TargetPath:      targetPath1,
				SkipOwners:      true,
				SkipPermissions: true,
			},
		})

		require.NoError(t, err)
		require.Equal(t, uitask.StatusSuccess, waitForTask(t, cli, restoreTask1.TaskID, 30*time.Second).Status)
		require.FileExists(t, filepath.Join(targetPath1, "file1.kopia-entry"))
		require.DirExists(t, filepath.Join(targetPath1, "dir1.kopia-entry"))
		require.FileExists(t, filepath.Join(targetPath1, "dir1.kopia-entry", ".kopia-entry"))
	})

	t.Run("FilesystemPartialShallowRestore", func(t *testing.T) {
		targetPath1 := testutil.TempDirectory(t)
		restoreTask1, err := serverapi.Restore(ctx, cli, &serverapi.RestoreRequest{
			Root: string(id11),
			Options: restore.Options{
				RestoreDirEntryAtDepth: 1,
			},
			Filesystem: &restore.FilesystemOutput{
				TargetPath:      targetPath1,
				SkipOwners:      true,
				SkipPermissions: true,
			},
		})

		require.NoError(t, err)
		require.Equal(t, uitask.StatusSuccess, waitForTask(t, cli, restoreTask1.TaskID, 30*time.Second).Status)
		require.FileExists(t, filepath.Join(targetPath1, "file1"))
		require.DirExists(t, filepath.Join(targetPath1, "dir1"))
		require.FileExists(t, filepath.Join(targetPath1, "dir1", "file2.kopia-entry"))
	})

	t.Run("ZipFile", func(t *testing.T) {
		outputZipFile := filepath.Join(testutil.TempDirectory(t), "test1.zip")
		restoreTask1, err := serverapi.Restore(ctx, cli, &serverapi.RestoreRequest{
			Root: string(id11),
			Options: restore.Options{
				RestoreDirEntryAtDepth: math.MaxInt32,
			},
			ZipFile: outputZipFile,
		})

		require.NoError(t, err)
		require.Equal(t, uitask.StatusSuccess, waitForTask(t, cli, restoreTask1.TaskID, 30*time.Second).Status)
		require.FileExists(t, outputZipFile)
	})

	t.Run("UncompressedZipFile", func(t *testing.T) {
		outputZipFile := filepath.Join(testutil.TempDirectory(t), "test1.zip")
		restoreTask1, err := serverapi.Restore(ctx, cli, &serverapi.RestoreRequest{
			Root: string(id11),
			Options: restore.Options{
				RestoreDirEntryAtDepth: math.MaxInt32,
			},
			ZipFile:         outputZipFile,
			UncompressedZip: true,
		})

		require.NoError(t, err)
		require.Equal(t, uitask.StatusSuccess, waitForTask(t, cli, restoreTask1.TaskID, 30*time.Second).Status)
		require.FileExists(t, outputZipFile)
	})

	t.Run("TarFile", func(t *testing.T) {
		outputTarFile := filepath.Join(testutil.TempDirectory(t), "test1.tar")
		restoreTask1, err := serverapi.Restore(ctx, cli, &serverapi.RestoreRequest{
			Root: string(id11),
			Options: restore.Options{
				RestoreDirEntryAtDepth: math.MaxInt32,
			},
			TarFile: outputTarFile,
		})

		require.NoError(t, err)
		require.Equal(t, uitask.StatusSuccess, waitForTask(t, cli, restoreTask1.TaskID, 30*time.Second).Status)
		require.FileExists(t, outputTarFile)
	})

	t.Run("InvalidRequest", func(t *testing.T) {
		requests := []*serverapi.RestoreRequest{
			{
				Root: string(id11),
				// no output
			},
			{
				// no root
				ZipFile: filepath.Join(testutil.TempDirectory(t), "test1.zip"),
			},
			{
				Root:    string(id11 + "bad"),
				ZipFile: filepath.Join(testutil.TempDirectory(t), "test1.zip"),
			},
			{
				Root:    string(id11),
				ZipFile: "/no/such/directory/" + uuid.NewString() + "/test1.zip",
			},
			{
				Root:    string(id11),
				TarFile: "/no/such/directory/" + uuid.NewString() + "/test1.tar",
			},
		}

		for _, req := range requests {
			_, err := serverapi.Restore(ctx, cli, req)

			var se apiclient.HTTPStatusError

			require.ErrorAs(t, err, &se)
		}
	})
}
