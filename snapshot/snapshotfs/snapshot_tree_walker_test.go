package snapshotfs_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

func TestSnapshotTreeWalker(t *testing.T) {
	var callbackCounter atomic.Int32

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	w, err := snapshotfs.NewTreeWalker(
		ctx,
		snapshotfs.TreeWalkerOptions{
			EntryCallback: func(ctx context.Context, entry fs.Entry, oid object.ID, entryPath string) error {
				callbackCounter.Add(1)
				return nil
			},
		})
	require.NoError(t, err)

	defer w.Close(ctx)

	sourceRoot := mockfs.NewDirectory()
	require.Error(t, w.Process(ctx, sourceRoot, "."))

	dir1 := sourceRoot.AddDir("dir1", 0o755)
	dir2 := sourceRoot.AddDir("dir2", 0o755)

	dir1.AddFile("file11", []byte{1, 2, 3}, 0o644)
	dir2.AddFile("file21", []byte{1, 2, 3, 4}, 0o644)
	dir2.AddFile("file22", []byte{1, 2, 3}, 0o644) // same content as dir11/file11

	// root directory, 2 subdirectories + 2 unique files (dir1/file11 === dir2/file22)
	const numUniqueObjects = 5

	u := snapshotfs.NewUploader(env.RepositoryWriter)
	man, err := u.Upload(ctx, sourceRoot, nil, snapshot.SourceInfo{})
	require.NoError(t, err)

	uploadedRoot, err := snapshotfs.SnapshotRoot(env.Repository, man)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.Flush(ctx))
	require.NoError(t, w.Process(ctx, uploadedRoot, "."))

	require.EqualValues(t, numUniqueObjects, callbackCounter.Load())

	require.NoError(t, w.Process(ctx, uploadedRoot, "."))

	// callback not invoked again
	require.EqualValues(t, numUniqueObjects, callbackCounter.Load())

	// add one more file, upload again
	dir2.AddFile("file23", []byte{1, 2, 3, 4, 5}, 0o644)

	man, err = u.Upload(ctx, sourceRoot, nil, snapshot.SourceInfo{})
	require.NoError(t, err)

	uploadedRoot, err = snapshotfs.SnapshotRoot(env.Repository, man)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.Flush(ctx))
	require.NoError(t, w.Process(ctx, uploadedRoot, "."))

	// uploading new object causes 3 new objects: 1 object, 1 update dir2, 1 updated root
	require.EqualValues(t, numUniqueObjects+3, callbackCounter.Load())
}

func TestSnapshotTreeWalker_Errors(t *testing.T) {
	someErr1 := errors.New("some error")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	w, err := snapshotfs.NewTreeWalker(
		ctx,
		snapshotfs.TreeWalkerOptions{
			Parallelism: 1,
			EntryCallback: func(ctx context.Context, entry fs.Entry, oid object.ID, entryPath string) error {
				if entryPath == "root-dir/dir2/file21" {
					return someErr1
				}

				return nil
			},
		})
	require.NoError(t, err)

	defer w.Close(ctx)

	sourceRoot := mockfs.NewDirectory()
	require.Error(t, w.Process(ctx, sourceRoot, "root-dir"))

	dir1 := sourceRoot.AddDir("dir1", 0o755)
	dir2 := sourceRoot.AddDir("dir2", 0o755)

	dir1.AddFile("file11", []byte{1, 2, 3}, 0o644)
	dir2.AddFile("file21", []byte{1, 2, 3, 4}, 0o644)
	dir2.AddFile("file22", []byte{1, 2, 3}, 0o644) // same content as dir11/file11

	u := snapshotfs.NewUploader(env.RepositoryWriter)
	man, err := u.Upload(ctx, sourceRoot, nil, snapshot.SourceInfo{})
	require.NoError(t, err)

	uploadedRoot, err := snapshotfs.SnapshotRoot(env.Repository, man)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.Flush(ctx))
	require.ErrorIs(t, w.Process(ctx, uploadedRoot, "root-dir"), someErr1)
}

func TestSnapshotTreeWalker_MultipleErrors(t *testing.T) {
	someErr1 := errors.New("some error")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	w, err := snapshotfs.NewTreeWalker(
		ctx,
		snapshotfs.TreeWalkerOptions{
			Parallelism: 1,
			MaxErrors:   -1,
			EntryCallback: func(ctx context.Context, entry fs.Entry, oid object.ID, entryPath string) error {
				if entryPath == "root-dir/dir1/file11" {
					return someErr1
				}

				if entryPath == "root-dir/dir2/file21" {
					return someErr1
				}

				return nil
			},
		})
	require.NoError(t, err)

	defer w.Close(ctx)

	sourceRoot := mockfs.NewDirectory()
	require.Error(t, w.Process(ctx, sourceRoot, "root-dir"))

	dir1 := sourceRoot.AddDir("dir1", 0o755)
	dir2 := sourceRoot.AddDir("dir2", 0o755)

	dir1.AddFile("file11", []byte{1, 2, 3}, 0o644)
	dir2.AddFile("file21", []byte{1, 2, 3, 4}, 0o644)
	dir2.AddFile("file22", []byte{1, 2, 3, 4, 5}, 0o644)

	u := snapshotfs.NewUploader(env.RepositoryWriter)
	man, err := u.Upload(ctx, sourceRoot, nil, snapshot.SourceInfo{})
	require.NoError(t, err)

	uploadedRoot, err := snapshotfs.SnapshotRoot(env.Repository, man)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	err = w.Process(ctx, uploadedRoot, "root-dir")
	require.Error(t, err)
	require.Equal(t, "encountered 2 errors", err.Error())
}

func TestSnapshotTreeWalker_MultipleErrorsSameOID(t *testing.T) {
	someErr1 := errors.New("some error")

	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)

	w, err := snapshotfs.NewTreeWalker(
		ctx,
		snapshotfs.TreeWalkerOptions{
			Parallelism: 1,
			MaxErrors:   -1,
			EntryCallback: func(ctx context.Context, entry fs.Entry, oid object.ID, entryPath string) error {
				if entryPath == "root-dir/dir1/file11" {
					return someErr1
				}

				if entryPath == "root-dir/dir2/file22" {
					return someErr1
				}

				return nil
			},
		})
	require.NoError(t, err)

	defer w.Close(ctx)

	sourceRoot := mockfs.NewDirectory()
	require.Error(t, w.Process(ctx, sourceRoot, "root-dir"))

	dir1 := sourceRoot.AddDir("dir1", 0o755)
	dir2 := sourceRoot.AddDir("dir2", 0o755)

	dir1.AddFile("file11", []byte{1, 2, 3}, 0o644)
	dir2.AddFile("file21", []byte{1, 2, 3, 4}, 0o644)
	dir2.AddFile("file22", []byte{1, 2, 3}, 0o644) // same content as dir11/file11

	u := snapshotfs.NewUploader(env.RepositoryWriter)
	man, err := u.Upload(ctx, sourceRoot, nil, snapshot.SourceInfo{})
	require.NoError(t, err)

	uploadedRoot, err := snapshotfs.SnapshotRoot(env.Repository, man)
	require.NoError(t, err)

	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	err = w.Process(ctx, uploadedRoot, "root-dir")
	require.Error(t, err)
	require.True(t, errors.Is(err, someErr1))
}
